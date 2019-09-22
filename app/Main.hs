{-# LANGUAGE DeriveGeneric, StandaloneDeriving, GeneralizedNewtypeDeriving
  , DeriveFunctor, MultiParamTypeClasses #-}
module Main where

import System.IO
import System.Environment (getArgs)
import qualified Streamly as S
import qualified Streamly.Prelude as S
import Control.DeepSeq
import GHC.Generics
import qualified Data.Csv as Csv
import qualified Data.ByteString as BS
import qualified Data.ByteString.Lazy as BSL
import qualified Data.Vector as V
import Control.Monad.Error.Class
import Control.Monad.Catch
import Control.Monad.IO.Class
import Streamly.Csv
import qualified Streaming as ST
import qualified Streaming.With as ST
import qualified Streaming.Prelude as ST
import qualified Streaming.Cassava as CsvST
import qualified Data.ByteString.Streaming as BSS

deriving instance Generic CsvParseException
instance NFData CsvParseException

main :: IO ()
main = do
  (mode:testfile:_) <- getArgs
  case mode of
      "streamly" ->
        copyAllStreamly testfile $ "out_"<>testfile
      "cassava" ->
        copyAllCassava testfile $ "out_"<>testfile
      "streaming" -> do
        recs <- readAllStreaming testfile
        print $ length (force recs)
      _ -> error "First argument should be 'streamly', 'streaming', or 'cassava'"


readAllStreamly :: FilePath -> IO [V.Vector  BS.ByteString]
readAllStreamly filename = do
  h <- openFile filename ReadMode
  let chunks = chunkStream h (64*1024)
      recs = decode Csv.HasHeader chunks
  S.toList recs


readAllCassava :: FilePath -> IO (V.Vector (V.Vector BS.ByteString))
readAllCassava filename = do
  contents <- BSL.readFile filename
  either (error . show) return $
    Csv.decode Csv.HasHeader contents


copyAllCassava :: FilePath -> FilePath -> IO ()
copyAllCassava fIn fOut = do
  contents <- BSL.readFile fIn
  recs <- either (error . show) return $
    Csv.decode Csv.HasHeader contents :: IO (V.Vector (V.Vector BS.ByteString))
  BSL.writeFile fOut $ Csv.encode $ V.toList recs


copyAllStreamly :: FilePath -> FilePath -> IO ()
copyAllStreamly fIn fOut = do
  h <- openFile fIn ReadMode
  let chunks = chunkStream h (64*1024)
      recs = decode Csv.HasHeader chunks :: S.SerialT IO (V.Vector BS.ByteString)
  withFile fOut WriteMode $ \ho ->
    S.mapM_ (BS.hPut ho) $ encode Nothing recs


newtype StupidMonad a = StupidMonad {runStupid :: IO a}
  deriving (Functor, Applicative, Monad, MonadIO, MonadMask
           , MonadCatch, MonadThrow)

instance MonadError CsvST.CsvParseException StupidMonad where
  throwError e = StupidMonad (throwM e)
  catchError (StupidMonad a) f = StupidMonad $ catch a (runStupid . f)

readAllStreaming :: FilePath -> IO [V.Vector BS.ByteString]
readAllStreaming filename = ST.withBinaryFileContents filename go
  where 
    go :: BSS.ByteString IO () -> IO [V.Vector BS.ByteString]
    go contents = 
      let recs :: ST.Stream (ST.Of (V.Vector BS.ByteString)) StupidMonad ()
          recs = CsvST.decode HasHeader (ST.hoist StupidMonad contents)
       in runStupid $ ST.toList_  recs
