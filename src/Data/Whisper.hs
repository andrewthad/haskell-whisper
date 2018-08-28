{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE DerivingStrategies #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE MagicHash #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE TypeInType #-}
module Data.Whisper
  ( Whisper(..)
  , Header(..)
  , Aggregation(..)
  , Metadata(..)
  , Archive(..)
  , ArchiveInfo(..)
  , WhisperError(..)
  , fromHandle
  , fromStream
  , headerFromHandle
  , headerFromStream
  ) where

import Control.Applicative (liftA3)
import Control.Monad.ST (stToIO,ST)
import Data.Primitive (Array,PrimArray)
import Data.Word (Word32,Word64)
import GHC.Int (Int(I#))
import Packed.Bytes.Parser (Parser)
import Packed.Bytes.Stream.ST (ByteStream)
import System.IO (Handle)

import qualified Packed.Bytes.Parser as P
import qualified Packed.Bytes.Stream.IO as PBSIO
import qualified Data.Primitive as PM

-- | Aggregation method used to roll up old metrics.
data Aggregation
  = Average
  | Sum
  | Last
  | Max
  | Min
  | AverageZero
  | AbsoluteMax
  | AbsoluteMin
  deriving (Show,Eq)

-- | All of the data present in a whisper file. This includes data
-- points with invalid timestamps. For reference, the @whisper-dump@
-- utility provides such data points as well. Typically, they need to
-- be filtered out.
data Whisper = Whisper
  { whisperHeader :: !Header
  , whisperArchives :: !(Array Archive)
  }

data Header = Header
  { headerMetadata :: !Metadata
  , headerArchiveInfos :: !(Array ArchiveInfo)
  }

data Metadata = Metadata
  { metadataAggregation :: !Aggregation
  , metadataMaximumRetention :: !Word32
  , metadataFilesFactor :: !Float
  , metadataArchiveCount :: !Word32
  }

-- | Archive is the timestamped data points represented as a structure
-- of arrays. These two arrays must have the same length.
data Archive = Archive
  { archiveTimestamps :: !(PrimArray Word32)
  , archiveValues :: !(PrimArray Double)
  }

data ArchiveInfo = ArchiveInfo
  { archiveInfoOffset :: !Word32
  , archiveInfoSecondsPerPoint :: !Word32
  , archiveInfoPoints :: !Word32
  }

-- | Description 
data WhisperError
  = WhisperErrorMetadata -- ^ stream ended while parsing opening metadata
  | WhisperErrorAggregation !Word32 -- ^ invalid aggregation type
  | WhisperErrorArchive !Int -- ^ stream ended while parsing archive information
  | WhisperErrorData !Int -- ^ stream ended while parsing archive data
  | WhisperErrorLeftovers -- ^ extra bytes at the end of the stream
  deriving stock (Show,Eq)

data WhisperMalformed
  = WhisperMalformedAggregation !Word32
  | WhisperMalformedLeftovers

data Phase
  = PhaseMetadata
  | PhaseArchive !Int
  | PhaseData !Int

-- | Read the contents of the whisper file. If the read is successful,
-- the position of the file descriptor will be the end of the file.
-- However, this function does not close the @Handle@. The caller of
-- this function has that responsibility.
fromHandle :: Handle -> IO (Either WhisperError Whisper)
fromHandle = stToIO . fromStream . PBSIO.fromHandle

headerFromHandle :: Handle -> IO (Either WhisperError Header)
headerFromHandle = stToIO . headerFromStream . PBSIO.fromHandle
  
fromStream :: ByteStream s -> ST s (Either WhisperError Whisper)
fromStream stream = do
  -- We are free to disregard the leftovers since, in the parser,
  -- we check to make sure we are at the end.
  P.Result _ e c <- P.parseStreamST stream PhaseMetadata parser
  pure (convertResult e c)
    
headerFromStream :: ByteStream s -> ST s (Either WhisperError Header)
headerFromStream stream = do
  P.Result _ e c <- P.parseStreamST stream PhaseMetadata parserHeader
  pure (convertResult e c)

convertResult :: Either (Maybe WhisperMalformed) a -> Phase -> Either WhisperError a
convertResult e c = case e of
  Left m -> case m of
    Nothing -> case c of
      PhaseMetadata -> Left WhisperErrorMetadata
      PhaseArchive ix -> Left (WhisperErrorArchive ix)
      PhaseData ix -> Left (WhisperErrorData ix)
    Just x -> case x of
      WhisperMalformedAggregation w -> Left (WhisperErrorAggregation w)
      WhisperMalformedLeftovers -> Left WhisperErrorLeftovers
  Right w -> Right w

parserHeader :: Parser WhisperMalformed Phase Header
parserHeader = do
  agg <- parserAggregation
  maxRet <- P.bigEndianWord32
  filesFactor <- P.bigEndianFloat
  archiveCount <- P.bigEndianWord32
  -- TODO: Make this replicate report errors correctly. Currently,
  -- it does not update the context.
  infos <- P.replicateIndex# (\ix -> PhaseArchive (I# ix)) (word32ToInt archiveCount) parserArchiveInfo
  pure (Header (Metadata agg maxRet filesFactor archiveCount) infos)

parser :: Parser WhisperMalformed Phase Whisper
parser = do
  h@(Header _ infos) <- parserHeader
  archives <- P.statefully $ PM.traverseArrayP
    ( \(ArchiveInfo offset secondsPerPoint points) -> do
      mutTimestamps <- P.mutation $ PM.newPrimArray (word32ToInt points)
      mutValues <- P.mutation $ PM.newPrimArray (word32ToInt points)
      let go !ix = if ix < word32ToInt points
            then do
              timestamp <- P.consumption (P.setContext (PhaseData ix) *> P.bigEndianWord32)
              P.mutation (PM.writePrimArray mutTimestamps 0 timestamp)
              value <- P.consumption P.bigEndianWord64
              P.mutation (PM.writePrimArray mutValues 0 value)
              go (ix + 1)
            else pure ()
      go 0
      timestamps <- P.mutation $ PM.unsafeFreezePrimArray mutTimestamps
      values <- P.mutation $ PM.unsafeFreezePrimArray mutValues
      let values' = coerceWord64ArrayToDoubleArray values
      pure (Archive timestamps values')
    ) infos
  P.isEndOfInput >>= \case
    True -> pure ()
    False -> P.failureDocumented WhisperMalformedLeftovers
  pure (Whisper h archives)
    
word32ToInt :: Word32 -> Int
word32ToInt = fromIntegral

coerceWord64ArrayToDoubleArray :: PrimArray Word64 -> PrimArray Double
coerceWord64ArrayToDoubleArray (PM.PrimArray x) = PM.PrimArray x

parserArchiveInfo :: Parser e c ArchiveInfo
parserArchiveInfo = liftA3 ArchiveInfo P.bigEndianWord32 P.bigEndianWord32 P.bigEndianWord32

parserAggregation :: Parser WhisperMalformed c Aggregation
parserAggregation = P.bigEndianWord32 >>= \case
  1 -> pure Average
  2 -> pure Sum
  3 -> pure Last
  4 -> pure Max
  5 -> pure Min
  6 -> pure AverageZero
  7 -> pure AbsoluteMax
  8 -> pure AbsoluteMin
  w -> P.failureDocumented (WhisperMalformedAggregation w)
