{-# LANGUAGE FlexibleContexts, MultiParamTypeClasses, OverloadedStrings,
             ScopedTypeVariables, LambdaCase #-}

{- |
   Module      : Streamly.Csv
   Description : Cassava support for the streamly library
   Copyright   : (c) Richard Warfield
   License     : BSD 3-clause
   Maintainer  : richard@litx.io

   Stream CSV data in\/out using
   [Cassava](http://hackage.haskell.org/package/cassava).  Adapted from
   [streaming-cassava](http://hackage.haskell.org/package/streaming-cassava).

   For efficiency, operates on streams of strict ByteString chunks 
   @(i.e. IsStream t => t m ByteString)@ rather than directly on streams of Word8. 
   The 'chunkStream' function is useful for generating an input stream from a
   'Handle'.

   Example usage:

   > import Streamly
   > import qualified Streamly.Prelude as S
   > import Streamly.Csv (decode, encode, chunkStream)
   > import System.IO
   > import qualified Data.Csv as Csv
   > import qualified Data.ByteString as BS
   > import Data.Vector (Vector)
   >
   > do
   >   h <- openFile "testfile.csv" ReadMode
   >   let chunks = chunkStream h (64*1024)
   >       recs = decode Csv.HasHeader chunks :: SerialT IO (Vector BS.ByteString)
   >   withFile "dest.csv" WriteMode $ \ho ->
   >     S.mapM_ (BS.hPut ho) $ encode Nothing recs
 -}
module Streamly.Csv
  ( -- * Decoding
    decode
  , decodeWith
  , decodeWithErrors
  , CsvParseException (..)
  , chunkStream
    -- ** Named decoding
  , decodeByName
  , decodeByNameWith
  , decodeByNameWithErrors
    -- * Encoding
  , encode
  , encodeDefault
  , encodeWith
    -- ** Named encoding
  , encodeByName
  , encodeByNameDefault
  , encodeByNameWith
    -- * Re-exports
  , FromRecord (..)
  , FromNamedRecord (..)
  , ToRecord (..)
  , ToNamedRecord (..)
  , DefaultOrdered (..)
  , HasHeader (..)
  , Header
  , header
  , Name
  , DecodeOptions(..)
  , defaultDecodeOptions
  , EncodeOptions(..)
  , defaultEncodeOptions
  ) where

import qualified Data.ByteString                    as BS
import qualified Data.ByteString.Lazy               as BSL
import           Streamly
import qualified Streamly.Prelude                  as S

import           Data.Csv             (DecodeOptions(..), DefaultOrdered(..),
                                       EncodeOptions(..), FromNamedRecord(..),
                                       FromRecord(..), Header, Name,
                                       ToNamedRecord(..), ToRecord(..),
                                       defaultDecodeOptions,
                                       defaultEncodeOptions, encIncludeHeader,
                                       header)
import           Data.Csv.Incremental (HasHeader(..), HeaderParser(..),
                                       Parser(..))
import qualified Data.Csv.Incremental as CI

import System.IO (Handle)
import Control.Exception         (Exception(..))
import Control.Monad.Catch (MonadThrow(..))
import Control.Monad.IO.Class (MonadIO, liftIO)
import Data.Word             (Word8)
import Data.Bifunctor            (first)
import Data.Maybe                (fromMaybe)
import Data.String               (IsString(..))
import Data.Typeable             (Typeable)

--------------------------------------------------------------------------------

-- | Use 'defaultOptions' for decoding the provided CSV.
decode :: (IsStream t, MonadAsync m, FromRecord a)
       => HasHeader
       -> t m BS.ByteString
       -> t m a
decode = decodeWith defaultDecodeOptions

-- | Return back a stream of values from the provided CSV, stopping at
--   the first error.
--
--   If you wish to instead ignore errors, consider using
--   'decodeWithErrors' with 'S.mapMaybe'
--
--   Any remaining input is discarded.
decodeWith :: (IsStream t, MonadAsync m, FromRecord a)
           => DecodeOptions -> HasHeader
           -> t m BS.ByteString
           -> t m a
decodeWith opts hdr chunks = getValues (decodeWithErrors opts hdr chunks)
                         -- >>= either (throwError . fst) return

-- | Return back a stream with an attempt at type conversion, and
--   either the previous result or any overall parsing errors with the
--   remainder of the input.
decodeWithErrors :: (IsStream t, Monad m, FromRecord a, MonadThrow m)
                 => DecodeOptions -> HasHeader
                 -> t m BS.ByteString
                 -> t m (Either CsvParseException a)
decodeWithErrors opts = runParser . CI.decodeWith opts

runParser :: forall t a m. (IsStream t, Monad m, MonadThrow m)
          => Parser a -> t m BS.ByteString -> t m (Either CsvParseException a)
runParser p chunked = S.concatMap fst $ S.scanlM' continue (S.nil, const p) $
                        S.cons BS.empty chunked
  where
    continue :: (t m (Either CsvParseException a), BS.ByteString -> Parser a)
             -> BS.ByteString
             -> m (t m (Either CsvParseException a), BS.ByteString -> Parser a)
    continue (_, p) chunk =
      case p chunk of
        Fail bs err -> throwM (CsvParseException err)
        Many es get -> return (withEach es, get)
        Done es     -> return (withEach es, p)

    withEach = S.fromList . map (first CsvParseException)

chunkStream :: (IsStream t, MonadAsync m) => Handle -> Int -> t m BS.ByteString
chunkStream h chunkSize = loop
  where
    loop = S.takeWhile (not . BS.null) $
      liftIO (BS.hGetSome h chunkSize) `S.consM` loop

--------------------------------------------------------------------------------

-- | Use 'defaultOptions' for decoding the provided CSV.
decodeByName :: (MonadAsync m, FromNamedRecord a)
                => SerialT m BS.ByteString -> SerialT m a
decodeByName = decodeByNameWith defaultDecodeOptions

-- | Return back a stream of values from the provided CSV, stopping at
--   the first error.
--
--   A header is required to determine the order of columns, but then
--   discarded.
--
--   If you wish to instead ignore errors, consider using
--   'decodeByNameWithErrors' with 'S.mapMaybe'
--
--   Any remaining input is discarded.
decodeByNameWith :: (MonadAsync m, FromNamedRecord a)
                    => DecodeOptions
                    -> SerialT m BS.ByteString -> SerialT m a
decodeByNameWith opts bs = getValues (decodeByNameWithErrors opts bs)
                           -- >>= either (throwError . fst) return

-- | Return back a stream with an attempt at type conversion, but
--   where the order of columns doesn't have to match the order of
--   fields of your actual type.
--
--   This requires\/assumes a header in the CSV stream, which is
--   discarded after parsing.
--
decodeByNameWithErrors :: forall m a. (Monad m, MonadThrow m, FromNamedRecord a) 
                       => DecodeOptions
                       -> SerialT m BS.ByteString
                       -> SerialT m (Either CsvParseException a)
decodeByNameWithErrors opts chunked = do
  (p, rest) <- S.yieldM $ extractParser (const $ CI.decodeByNameWith opts) $ S.cons BS.empty chunked
  runParser p rest
  where
    extractParser :: (BS.ByteString -> HeaderParser (Parser a))
                  -> SerialT m BS.ByteString
                  -> m (Parser a, SerialT m BS.ByteString)
    extractParser p chunks = S.uncons chunks >>= \case
      Just (hed, rest) -> 
        case p hed of
          FailH bs err -> throwM (CsvParseException err)
          PartialH get -> extractParser get rest
          DoneH _ p    -> return (p, rest)
      Nothing -> throwM $ CsvParseException "Unexpected end of input stream"

-- --------------------------------------------------------------------------------
-- 
-- -- | Encode a stream of values with the default options.
-- --
-- --   Optionally prefix the stream with headers (the 'header' function
-- --   may be useful).
encode :: (IsStream t, ToRecord a, Monad m) => Maybe Header
          -> t m a -> t m BS.ByteString
encode = encodeWith defaultEncodeOptions

-- | Encode a stream of values with the default options and a derived
--   header prefixed.
encodeDefault :: forall a t m. (IsStream t, ToRecord a, DefaultOrdered a, Monad m)
                 => t m a -> t m BS.ByteString
encodeDefault = encode (Just (headerOrder (undefined :: a)))

-- | Encode a stream of values with the provided options.
--
--   Optionally prefix the stream with headers (the 'header' function
--   may be useful).
encodeWith :: (IsStream t, ToRecord a, Monad m)
           => EncodeOptions 
           -> Maybe Header
           -> t m a 
           -> t m BS.ByteString
encodeWith opts mhdr = S.concatMap S.fromList
                       . addHeaders
                       . S.map enc
  where
    addHeaders = maybe id (S.cons . enc) mhdr

    enc :: (ToRecord v) => v -> [BS.ByteString]
    enc = BSL.toChunks . CI.encodeWith opts . CI.encodeRecord

--------------------------------------------------------------------------------

-- | Use the default ordering to encode all fields\/columns.
encodeByNameDefault :: forall a t m. (IsStream t, DefaultOrdered a, ToNamedRecord a, Monad m)
                       => t m a -> t m BS.ByteString
encodeByNameDefault = encodeByName (headerOrder (undefined :: a))

-- | Select the columns that you wish to encode from your data
--   structure using default options (which currently includes
--   printing the header).
encodeByName :: (IsStream t, ToNamedRecord a, Monad m) => Header
                -> t m a -> t m BS.ByteString
encodeByName = encodeByNameWith defaultEncodeOptions

-- | Select the columns that you wish to encode from your data
--   structure.
--
--   Header printing respects 'encIncludeheader'.
encodeByNameWith :: (IsStream t, ToNamedRecord a, Monad m) => EncodeOptions -> Header
                    -> t m a -> t m BS.ByteString
encodeByNameWith opts hdr = S.concatMap S.fromList
                            . addHeaders
                            . S.map enc
  where
    opts' = opts { encIncludeHeader = False }

    addHeaders
      | encIncludeHeader opts = S.cons . BSL.toChunks
                                . CI.encodeWith opts' . CI.encodeRecord $ hdr
      | otherwise             = id

    enc = BSL.toChunks . CI.encodeByNameWith opts' hdr . CI.encodeNamedRecord

--------------------------------------------------------------------------------

getValues :: (IsStream t, MonadAsync m, Exception e)
          => t m (Either e a) -> t m a
getValues = S.mapM (either throwM return)

newtype CsvParseException = CsvParseException String
  deriving (Eq, Show, Typeable)

instance IsString CsvParseException where
  fromString = CsvParseException

instance Exception CsvParseException where
  displayException (CsvParseException e) = "Error parsing csv: " ++ e

