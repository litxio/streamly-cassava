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

   > import qualified Streamly.Data.Stream.Prelude  as S
   > import qualified Streamly.Internal.Data.Stream as SI
   > import Streamly.Csv (decode, encode, chunkStream)
   > import System.IO
   > import qualified Data.Csv as Csv
   > import qualified Data.ByteString as BS
   > import Data.Vector (Vector)
   >
   > do
   >   h <- openFile "testfile.csv" ReadMode
   >   let chunks = chunkStream h (64*1024)
   >       recs = decode Csv.HasHeader chunks :: S.Stream IO (Vector BS.ByteString)
   >   withFile "dest.csv" WriteMode $ \ho ->
   >     SI.mapM_ (BS.hPut ho) $ encode Nothing recs
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
import qualified Streamly.Data.Stream.Prelude       as S
import qualified Streamly.Internal.Data.Stream      as SI
import Streamly.Data.Stream.Prelude (MonadAsync, Stream)

import Control.Exception         (Exception(..))
import Control.Monad.Catch       (MonadThrow(..))
import Control.Monad.IO.Class    (MonadIO, liftIO)
import Data.Bifunctor            (first)
import Data.Csv                  (DecodeOptions(..), DefaultOrdered(..),
                                  EncodeOptions(..), FromNamedRecord(..),
                                  FromRecord(..), Header, Name,
                                  ToNamedRecord(..), ToRecord(..),
                                  defaultDecodeOptions,
                                  defaultEncodeOptions, encIncludeHeader,
                                  header)
import Data.Csv.Incremental      (HasHeader(..), HeaderParser(..), Parser(..))
import qualified Data.Csv.Incremental as CI
import Data.Maybe                (fromMaybe)
import Data.String               (IsString(..))
import Data.Typeable             (Typeable)
import Data.Word                 (Word8)
import System.IO                 (Handle)

--------------------------------------------------------------------------------

-- | Use 'defaultOptions' for decoding the provided CSV.
decode :: (MonadAsync m, FromRecord a)
       => HasHeader
       -> Stream m BS.ByteString
       -> Stream m a
decode = decodeWith defaultDecodeOptions

-- | Return back a stream of values from the provided CSV, stopping at
--   the first error.
--
--   If you wish to instead ignore errors, consider using
--   'decodeWithErrors' with 'S.mapMaybe'
--
--   Any remaining input is discarded.
decodeWith :: (MonadAsync m, FromRecord a)
           => DecodeOptions -> HasHeader
           -> Stream m BS.ByteString
           -> Stream m a
decodeWith opts hdr chunks = getValues (decodeWithErrors opts hdr chunks)
                         -- >>= either (throwError . fst) return

-- | Return back a stream with an attempt at type conversion, and
--   either the previous result or any overall parsing errors with the
--   remainder of the input.
decodeWithErrors :: (Monad m, FromRecord a, MonadThrow m)
                 => DecodeOptions -> HasHeader
                 -> Stream m BS.ByteString
                 -> Stream m (Either CsvParseException a)
decodeWithErrors opts = runParser . CI.decodeWith opts

runParser :: forall a m. (Monad m, MonadThrow m)
          => Parser a -> Stream m BS.ByteString -> Stream m (Either CsvParseException a)
runParser p chunked = S.concatMap fst $ SI.scanlM' continue (pure (S.nil, const p)) $
                        S.cons BS.empty chunked
  where
    continue :: (Stream m (Either CsvParseException a), BS.ByteString -> Parser a)
             -> BS.ByteString
             -> m (Stream m (Either CsvParseException a), BS.ByteString -> Parser a)
    continue (_, p) chunk =
      case p chunk of
        Fail bs err -> throwM (CsvParseException err)
        Many es get -> return (withEach es, get)
        Done es     -> return (withEach es, p)

    withEach = S.fromList . map (first CsvParseException)

chunkStream :: (MonadAsync m)
            => Handle -> Int -> Stream m BS.ByteString
chunkStream h chunkSize =
  S.takeWhile (not . BS.null) $ S.repeatM (liftIO $ BS.hGetSome h chunkSize)

--------------------------------------------------------------------------------

-- | Use 'defaultOptions' for decoding the provided CSV.
decodeByName :: (MonadAsync m, FromNamedRecord a)
                => Stream m BS.ByteString -> Stream m a
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
                    -> Stream m BS.ByteString -> Stream m a
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
                       -> Stream m BS.ByteString
                       -> Stream m (Either CsvParseException a)
decodeByNameWithErrors opts chunked =
  SI.concat $ S.fromEffect
    $ uncurry runParser <$>
        extractParser (const $ CI.decodeByNameWith opts) (S.cons BS.empty chunked)
                    -- >>= uncurry runParser
  where
    extractParser :: (BS.ByteString -> HeaderParser (Parser a))
                  -> Stream m BS.ByteString
                  -> m (Parser a, Stream m BS.ByteString)
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
encode :: (ToRecord a, Monad m) => Maybe Header
          -> Stream m a -> Stream m BS.ByteString
encode = encodeWith defaultEncodeOptions

-- | Encode a stream of values with the default options and a derived
--   header prefixed.
encodeDefault :: forall a m. (ToRecord a, DefaultOrdered a, Monad m)
                 => Stream m a -> Stream m BS.ByteString
encodeDefault = encode (Just (headerOrder (undefined :: a)))

-- | Encode a stream of values with the provided options.
--
--   Optionally prefix the stream with headers (the 'header' function
--   may be useful).
encodeWith :: (ToRecord a, Monad m)
           => EncodeOptions
           -> Maybe Header
           -> Stream m a
           -> Stream m BS.ByteString
encodeWith opts mhdr = S.concatMap S.fromList
                       . addHeaders
                       . SI.map enc
  where
    addHeaders = maybe id (S.cons . enc) mhdr

    enc :: (ToRecord v) => v -> [BS.ByteString]
    enc = BSL.toChunks . CI.encodeWith opts . CI.encodeRecord

--------------------------------------------------------------------------------

-- | Use the default ordering to encode all fields\/columns.
encodeByNameDefault :: forall a m. (DefaultOrdered a, ToNamedRecord a, Monad m)
                       => Stream m a -> Stream m BS.ByteString
encodeByNameDefault = encodeByName (headerOrder (undefined :: a))

-- | Select the columns that you wish to encode from your data
--   structure using default options (which currently includes
--   printing the header).
encodeByName :: (ToNamedRecord a, Monad m) => Header
                -> Stream m a -> Stream m BS.ByteString
encodeByName = encodeByNameWith defaultEncodeOptions

-- | Select the columns that you wish to encode from your data
--   structure.
--
--   Header printing respects 'encIncludeheader'.
encodeByNameWith :: (ToNamedRecord a, Monad m) => EncodeOptions -> Header
                    -> Stream m a -> Stream m BS.ByteString
encodeByNameWith opts hdr = S.concatMap S.fromList
                            . addHeaders
                            . SI.map enc
  where
    opts' = opts { encIncludeHeader = False }

    addHeaders
      | encIncludeHeader opts = S.cons . BSL.toChunks
                                . CI.encodeWith opts' . CI.encodeRecord $ hdr
      | otherwise             = id

    enc = BSL.toChunks . CI.encodeByNameWith opts' hdr . CI.encodeNamedRecord

--------------------------------------------------------------------------------

getValues :: (MonadAsync m, Exception e)
          => Stream m (Either e a) -> Stream m a
getValues = S.mapM (either throwM return)

newtype CsvParseException = CsvParseException String
  deriving (Eq, Show, Typeable)

instance IsString CsvParseException where
  fromString = CsvParseException

instance Exception CsvParseException where
  displayException (CsvParseException e) = "Error parsing csv: " ++ e

