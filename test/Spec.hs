{-# LANGUAGE DeriveGeneric, FlexibleContexts, MultiParamTypeClasses, RankNTypes,
             ScopedTypeVariables #-}

{-# OPTIONS_GHC -Wno-unused-binds #-}

{- |
   Module      : Main
   Description : Round-trip property testing
   Copyright   : (c) Ivan Lazar Miljenovic
   License     : MIT
   Maintainer  : Ivan.Miljenovic@gmail.com



 -}

import Streamly.Csv

import Streamly
import qualified Streamly.Prelude as S

import Test.Hspec                (describe, hspec)
import Test.Hspec.QuickCheck     (prop)
import Test.QuickCheck           (Arbitrary(..))
import Test.QuickCheck.Monadic
import Test.QuickCheck.Instances ()

import           Control.Monad.Catch (try, MonadCatch(..), SomeException)
import           Data.Text            (Text)
import qualified Data.Vector          as V
import           GHC.Generics         (Generic)

--------------------------------------------------------------------------------

main :: IO ()
main = hspec $ do
  describe "Plain records" $ do
    prop "Just data" $ \recs -> 
      monadicIO $ run (useType encodeDecode recs) >>= assert
    prop "With headers" $ \recs ->
      monadicIO $ run (useType encodeDecodeHeader recs) >>= assert
  describe "Named records" $ do
    prop "Default order" $ \recs ->
      monadicIO $ run (useType encodeDecodeNamed recs) >>= assert
    prop "Reversed order" $ \recs ->
      monadicIO $ run (useType encodeDecodeNamedReordered recs) >>= assert

encodeDecode :: (FromRecord a, ToRecord a, Eq a, MonadAsync m, MonadCatch m)
             => [a] -> m Bool
encodeDecode = encodeDecodeWith (decode NoHeader . encode Nothing)

encodeDecodeHeader :: (DefaultOrdered a, FromRecord a, ToRecord a, Eq a
                      , MonadAsync m, MonadCatch m)
                      => [a] -> m Bool
encodeDecodeHeader = encodeDecodeWith (decode HasHeader . encodeDefault)

encodeDecodeNamed :: (DefaultOrdered a, FromNamedRecord a, ToNamedRecord a
                     , Eq a, MonadAsync m, MonadCatch m)
                     => [a] -> m Bool
encodeDecodeNamed = encodeDecodeWith (decodeByName . encodeByNameDefault)

encodeDecodeNamedReordered :: forall a m. (DefaultOrdered a, FromNamedRecord a
                                          ,ToNamedRecord a, Eq a, MonadAsync m, MonadCatch m)
                              => [a] -> m Bool
encodeDecodeNamedReordered = encodeDecodeWith (decodeByName . encodeByName hdr)
  where
    hdr = V.reverse (headerOrder (undefined :: a))

encodeDecodeWith :: forall a m. (Eq a, MonadAsync m, MonadCatch m)
                    => (SerialT m a -> SerialT m a)
                    -> [a] -> m Bool
encodeDecodeWith f as = fmap (either (const False) (as==))
                        . (try :: m [a] -> m (Either SomeException [a]))
                        . S.toList
                        . f
                        . S.fromList
                        $ as

useType :: ([Test] -> r) -> [Test] -> r
useType = id

data Test = Test
  { columnA            :: !Int
  , longer_column_name :: !Text
  , mebbe              :: !(Maybe Double)
  } deriving (Eq, Show, Read, Generic)

-- DeriveAnyClass doesn't work with these types because of the Maybe

instance FromRecord Test
instance ToRecord Test
instance DefaultOrdered Test
instance FromNamedRecord Test
instance ToNamedRecord Test

instance Arbitrary Test where
  arbitrary = Test <$> arbitrary <*> arbitrary <*> arbitrary
