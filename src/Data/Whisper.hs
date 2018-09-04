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
  , clean
  , scrub
  ) where

import Control.Applicative (liftA3)
import Control.Monad.ST (stToIO,ST,runST)
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
  P.Result _ e <- P.parseStreamST stream parser
  pure e
    
headerFromStream :: ByteStream s -> ST s (Either WhisperError Header)
headerFromStream stream = do
  P.Result _ e <- P.parseStreamST stream parserHeader
  pure e

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

parserHeader :: Parser WhisperError Header
parserHeader = do
  agg <- parserAggregation
  maxRet <- P.bigEndianWord32 WhisperErrorMetadata
  filesFactor <- P.bigEndianFloat WhisperErrorMetadata
  archiveCount <- P.bigEndianWord32 WhisperErrorMetadata
  infos <- P.replicateIndex# (\ix -> WhisperErrorArchive (I# ix)) (word32ToInt archiveCount) parserArchiveInfo
  pure (Header (Metadata agg maxRet filesFactor archiveCount) infos)

parser :: Parser WhisperError Whisper
parser = do
  h@(Header _ infos) <- parserHeader
  archives <- P.statefully $ PM.traverseArrayP
    ( \(ArchiveInfo offset secondsPerPoint points) -> do
      mutTimestamps <- P.mutation $ PM.newPrimArray (word32ToInt points)
      mutValues <- P.mutation $ PM.newPrimArray (word32ToInt points)
      let go !ix = if ix < word32ToInt points
            then do
              timestamp <- P.consumption (P.bigEndianWord32 (WhisperErrorData ix))
              P.mutation (PM.writePrimArray mutTimestamps ix timestamp)
              value <- P.consumption (P.bigEndianWord64 (WhisperErrorData ix))
              P.mutation (PM.writePrimArray mutValues ix value)
              go (ix + 1)
            else pure ()
      go 0
      timestamps <- P.mutation $ PM.unsafeFreezePrimArray mutTimestamps
      values <- P.mutation $ PM.unsafeFreezePrimArray mutValues
      let values' = coerceWord64ArrayToDoubleArray values
      pure (Archive timestamps values')
    ) infos
  P.endOfInput WhisperErrorLeftovers
  pure (Whisper h archives)
    
word32ToInt :: Word32 -> Int
word32ToInt = fromIntegral

coerceWord64ArrayToDoubleArray :: PrimArray Word64 -> PrimArray Double
coerceWord64ArrayToDoubleArray (PM.PrimArray x) = PM.PrimArray x

parserArchiveInfo :: e -> Parser e ArchiveInfo
parserArchiveInfo e = liftA3 ArchiveInfo
  (P.bigEndianWord32 e)
  (P.bigEndianWord32 e)
  (P.bigEndianWord32 e)

parserAggregation :: Parser WhisperError Aggregation
parserAggregation = P.bigEndianWord32 WhisperErrorMetadata >>= \case
  1 -> pure Average
  2 -> pure Sum
  3 -> pure Last
  4 -> pure Max
  5 -> pure Min
  6 -> pure AverageZero
  7 -> pure AbsoluteMax
  8 -> pure AbsoluteMin
  w -> P.failure (WhisperErrorAggregation w)

-- | Remove all timestamp-value pairs that live at invalid offsets.
-- The presence of such pairs does not mean that the file is corrupted.
-- It means that the pair should not be treated as active data.
clean :: Whisper -> Whisper
clean (Whisper h@(Header _ infos) archives) = Whisper h $ imapArray
  ( \ix archive -> cleanArchive (archiveInfoSecondsPerPoint (PM.indexArray infos ix)) archive
  ) archives

-- | Similar to 'clean'. However, it also removes any pairs that have @0@ as the value.
-- This function does not exist for fundamental reasons, but it is occassionally a useful
-- convenience.
scrub :: Whisper -> Whisper
scrub (Whisper h@(Header _ infos) archives) = Whisper h $ imapArray
  ( \ix archive -> scrubArchive (archiveInfoSecondsPerPoint (PM.indexArray infos ix)) archive
  ) archives

imapArray :: (Int -> a -> b) -> Array a -> Array b
imapArray f a = runST $ do
  mb <- PM.newArray (PM.sizeofArray a) (error "Data.Whisper.imapArray: logic error")
  let go i | i == PM.sizeofArray a = return ()
           | otherwise = do
               x <- PM.indexArrayM a i
               PM.writeArray mb i (f i x) >> go (i+1)
  go 0
  PM.unsafeFreezeArray mb

emptyArchive :: Archive
emptyArchive = Archive mempty mempty

cleanArchive :: Word32 -> Archive -> Archive
cleanArchive !interval (Archive timestamps values)
  | PM.sizeofPrimArray timestamps == 0 = emptyArchive
  | otherwise =
      let !t0 = PM.indexPrimArray timestamps 0
       in filterArchive interval t0 timestamps values

scrubArchive :: Word32 -> Archive -> Archive
scrubArchive !interval (Archive timestamps values)
  | PM.sizeofPrimArray timestamps == 0 = emptyArchive
  | otherwise =
      let !t0 = PM.indexPrimArray timestamps 0
       in filterArchivePlus interval t0 timestamps values

filterArchivePlus :: 
     Word32 -- interval
  -> Word32 -- initial timestamp
  -> PrimArray Word32 -- timestamps
  -> PrimArray Double -- values
  -> Archive
filterArchivePlus !interval !t0 !timestamps !values = runST $ do
  let !sz = PM.sizeofPrimArray timestamps
  mtimestamps <- PM.newPrimArray sz
  mvalues <- PM.newPrimArray sz
  let go !ixSrc !ixDst = if ixSrc < sz
        then do
          let !t = PM.indexPrimArray timestamps ixSrc
              !v = PM.indexPrimArray values ixSrc
          if fromIntegral t == fromIntegral t0 + (fromIntegral interval * ixSrc) && v /= 0.0
            then do
              PM.writePrimArray mtimestamps ixDst t
              PM.writePrimArray mvalues ixDst v
              go (ixSrc + 1) (ixDst + 1)
            else go (ixSrc + 1) ixDst
        else return ixDst
  dstLen <- go 0 0
  mtimestamps' <- PM.resizeMutablePrimArray mtimestamps dstLen
  timestamps' <- PM.unsafeFreezePrimArray mtimestamps'
  mvalues' <- PM.resizeMutablePrimArray mvalues dstLen
  values' <- PM.unsafeFreezePrimArray mvalues'
  return (Archive timestamps' values')


filterArchive :: 
     Word32 -- interval
  -> Word32 -- initial timestamp
  -> PrimArray Word32 -- timestamps
  -> PrimArray Double -- values
  -> Archive
filterArchive !interval !t0 !timestamps !values = runST $ do
  let !sz = PM.sizeofPrimArray timestamps
  mtimestamps <- PM.newPrimArray sz
  mvalues <- PM.newPrimArray sz
  let go !ixSrc !ixDst = if ixSrc < sz
        then do
          let !t = PM.indexPrimArray timestamps ixSrc 
          if fromIntegral t == fromIntegral t0 + (fromIntegral interval * ixSrc)
            then do
              PM.writePrimArray mtimestamps ixDst t
              PM.writePrimArray mvalues ixDst (PM.indexPrimArray values ixSrc)
              go (ixSrc + 1) (ixDst + 1)
            else go (ixSrc + 1) ixDst
        else return ixDst
  dstLen <- go 0 0
  mtimestamps' <- PM.resizeMutablePrimArray mtimestamps dstLen
  timestamps' <- PM.unsafeFreezePrimArray mtimestamps'
  mvalues' <- PM.resizeMutablePrimArray mvalues dstLen
  values' <- PM.unsafeFreezePrimArray mvalues'
  return (Archive timestamps' values')


