# streamly-cassava

Stream CSV data in\/out using
[Cassava](http://hackage.haskell.org/package/cassava).  Adapted from
[streaming-cassava](http://hackage.haskell.org/package/streaming-cassava).

For efficiency, operates on streams of strict ByteString chunks 
(i.e. `Stream m ByteString`) rather than directly on streams of Word8. 
The chunkStream function is useful for generating an input stream from a
handle.

Example usage:

```haskell
import qualified Streamly.Data.Stream.Prelude  as S
import qualified Streamly.Internal.Data.Stream as SI                          ```
import Streamly.Csv (decode, encode, chunkStream)
import System.IO
import qualified Data.Csv as Csv
import qualified Data.ByteString as BS
import Data.Vector (Vector)
                                                                              
do
  h <- openFile "testfile.csv" ReadMode
  let chunks = chunkStream h (64*1024)
      recs = decode Csv.HasHeader chunks :: S.Stream IO (Vector BS.ByteString)
  withFile "dest.csv" WriteMode $ \ho ->
    SI.mapM_ (BS.hPut ho) $ encode Nothing recs
