module Database.MonetDB.Mapi
    (
    -- * Error handling
      MapiError
    , errorType
    , ErrorType(..)

    -- * Connection parameters
    , Lang(..)
    , ConInfo(..)
    , emptyConInfo
    , defConInfo

    -- * Connection handling
    , Connection(..)
    , connect
    , disconnect
    , withConnection

    -- * Queries
    , quickQuery
    ) where

import           Control.Monad
import           Data.Typeable
import           Foreign.C.String
import           Foreign.C.Types
import           Foreign.Concurrent    as C
import           Foreign.ForeignPtr
import           Foreign.Ptr
import qualified Bindings.MonetDB.Mapi as B
import qualified Control.Exception     as E

-- | Some IO actions might throw 'MapiError'.
data MapiError = MapiError
               { errorType    :: ErrorType
               }
               deriving Show

instance E.Exception MapiError where
    toException                       = E.SomeException
    fromException (E.SomeException e) = cast e

data ErrorType = Default String
               | Timeout
               | Server String
               deriving Show

-- | The language to be used for a connection.
data Lang = Sql
          | Mil
          | Mal
          | XQuery

fromLang :: Lang -> String
fromLang l = case l of
    Sql    -> "sql"
    Mil    -> "mil"
    Mal    -> "mal"
    XQuery -> "xquery"

newtype Connection = Connection B.Mapi

optCString :: Maybe String -> IO CString
optCString = maybe (return nullPtr) newCString

throwOnMapiError :: B.Mapi -> IO ()
throwOnMapiError mapi = do
    e <- B.mapi_error mapi
    -- Encountered MERROR
    if e == B.cMERROR
    then do
        cstr <- B.mapi_error_str mapi
        print cstr
        s <- peekCString cstr
        E.throwIO $ MapiError $ Default s
    else when (e == B.cMTIMEOUT) $ E.throwIO $ MapiError Timeout

throwOnMapiHdlError :: B.Mapi -> B.MapiHdl -> IO ()
throwOnMapiHdlError mapi hdl = do
    e <- B.mapi_error mapi
    when (e == B.cMSERVER) $
        getServerErrorMsg hdl >>= E.throwIO . MapiError . Server

getServerErrorMsg :: B.MapiHdl -> IO String
getServerErrorMsg hdl = do
    cstr <- B.mapi_result_error hdl
    peekCString cstr

throwOnError :: B.Mapi -> B.MapiHdl -> IO ()
throwOnError mapi hdl = do
    throwOnMapiError mapi
    throwOnMapiHdlError mapi hdl

data ConInfo = ConInfo
             { ciOptHost  :: Maybe String
             , ciOptPort  :: Maybe Int
             , ciUsername :: Maybe String
             , ciPassword :: Maybe String
             , ciLang     :: Lang
             , ciDbName   :: String
             }

-- | It is strongly advised to use 'withConnection', which is exception-safe.
connect :: ConInfo -> IO Connection
connect (ConInfo mHost mPort mUser mPass lang dbName) = do
    host <- optCString mHost
    let port = maybe 0 fromIntegral mPort
    user <- optCString mUser
    pass <- optCString mPass
    langStr <- newCString (fromLang lang)
    db <- newCString dbName
    mapi <- B.mapi_connect host port user pass langStr db
    throwOnMapiError mapi
    pure $ Connection mapi

disconnect :: Connection -> IO ()
disconnect (Connection mapi) = B.mapi_disconnect mapi

-- | A 'ConInfo' with all optional fields omitted.
emptyConInfo :: Lang -> String -> ConInfo
emptyConInfo = ConInfo Nothing Nothing Nothing Nothing

-- | A 'ConInfo' with common default settings, mostly for testing purposes.
defConInfo :: Lang -> String -> ConInfo
defConInfo = ConInfo Nothing Nothing (Just "monetdb") (Just "monetdb")

-- | Opens a connection, runs the action and closes the connection, even when
-- an exception has been thrown.
--
-- >>> withConnection (defConInfo Mal "test") $ \c -> quickQuery c "io.print(42, 23);"
-- [["42", "23"]]
--
withConnection :: ConInfo -> (Connection -> IO a) -> IO a
withConnection ci f = E.bracket (connect ci) disconnect f


-- TODO rework Result and Query, that mechanism is terrible

type QueryForeignPtr = ForeignPtr ()

newtype Query = Query QueryForeignPtr

query :: Connection -> String -> IO Query
query (Connection mapi) qStr = do
    qCStr <- newCString qStr
    mapiHdl <- B.mapi_query mapi qCStr
    throwOnError mapi mapiHdl
    q <- C.newForeignPtr mapiHdl (B.mapi_close_handle mapiHdl)
    pure $ Query q

newtype Result = Result QueryForeignPtr

execute :: Query -> IO Result
execute (Query fMapiHdl) = do
    withForeignPtr fMapiHdl $ \mapiHdl -> do
        B.mapi_execute mapiHdl
        -- TODO check for errors
    return $ Result fMapiHdl

fetchRow :: Result -> IO (Maybe [String])
fetchRow (Result fMapiHdl) = 
    withForeignPtr fMapiHdl $ \mapiHdl -> do
        nFields <- B.mapi_fetch_row mapiHdl
        if nFields == 0
        then return Nothing
        else do
            -- TODO mapi_fetch_field returns 0 on error
            let fetchField i = do
                    pc <- B.mapi_fetch_field mapiHdl i
                    sz <- B.mapi_fetch_field_len mapiHdl i
                    peekCStringLen (pc, fromIntegral sz)
            Just <$> mapM fetchField [0 .. pred nFields]

fetchRows :: Result -> IO [[String]]
fetchRows res = go
  where
    go = do
        mRow <- fetchRow res
        maybe (return []) (\row -> (row :) <$> go) mRow

fieldNames :: Result -> IO [String]
fieldNames (Result fMapiHdl) = withForeignPtr fMapiHdl $ \mapiHdl -> do
    nFields <- B.mapi_get_field_count mapiHdl
    -- FIXME may return 0
    fieldCStrs <- mapM (B.mapi_get_name mapiHdl) [0 .. pred nFields]
    mapM peekCString fieldCStrs

-- | Run a query and fetch the result rows.
quickQuery :: Connection -> String -> IO [[String]]
quickQuery c s = do
    q <- query c s
    r <- execute q
    fetchRows r

quickQuery_ :: Connection -> String -> IO ()
quickQuery_ c s = query c s >>= void . execute
