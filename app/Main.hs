{-# language BangPatterns #-}
{-# language LambdaCase #-}
{-# language OverloadedStrings #-}

import Control.Monad (forM_)
import Data.Functor (($>))
import Options.Applicative ((<**>))
import System.Exit (exitFailure)
import System.Directory (listDirectory,doesDirectoryExist)
import Data.List (intercalate,stripPrefix)
import Data.Double.Conversion.ByteString (toFixed)
import Data.ByteString (ByteString)

import qualified Data.ByteString.Char8 as BC
import qualified Data.ByteString.Builder as BB
import qualified Data.Whisper as W
import qualified Options.Applicative as P
import qualified System.IO as IO
import qualified Data.Primitive as PM
import qualified Data.Text.Lazy.IO as LTIO
import qualified Data.Text.Lazy.Builder as TB
import qualified Data.Text.Lazy.Builder.RealFloat as TBRF
import qualified Data.Text.Lazy.Builder.Int as TBI
import qualified Network.Socket.ByteString.Lazy as NSLB
import qualified Network.Socket as NS

main :: IO ()
main = do
  cmd <- P.execParser $ P.info
    (commandParser <**> P.helper)
    P.fullDesc
  case cmd of
    CommandHeader path -> runHeader path
    CommandData clean path -> runData clean path
    CommandSend dir carbon -> runSend dir carbon
    CommandList dir -> runList dir
      
runHeader :: String -> IO ()
runHeader path = IO.withFile path IO.ReadMode W.headerFromHandle >>= \case
  Left err -> do
    LTIO.hPutStrLn IO.stderr (TB.toLazyText (showError err))
    exitFailure
  Right header -> LTIO.putStr (TB.toLazyText (builderHeader header))

runData :: Bool -> String -> IO ()
runData clean path = IO.withFile path IO.ReadMode W.fromHandle >>= \case
  Left err -> do
    LTIO.hPutStrLn IO.stderr (TB.toLazyText (showError err))
    exitFailure
  Right whisper0 ->
    let whisper1 = if clean then W.clean whisper0 else whisper0
     in LTIO.putStr (TB.toLazyText (builderData whisper1))

runList :: String -> IO ()
runList root = traverseWhisperDatabase_ root (putStrLn . intercalate ".")

runSend :: String -> String -> IO ()
runSend root carbon = do
  let hints = NS.defaultHints { NS.addrSocketType = NS.Stream }
  addr:_ <- NS.getAddrInfo (Just hints) (Just carbon) (Just "2003")
  sock <- NS.socket (NS.addrFamily addr) (NS.addrSocketType addr) (NS.addrProtocol addr)
  NS.connect sock (NS.addrAddress addr)
  traverseWhisperDatabase_ root $ \subpath -> do
    let path = root ++ "/" ++ intercalate "/" subpath ++ ".wsp"
        metric = intercalate "." subpath
        metricBytes = BC.pack metric
    IO.withFile path IO.ReadMode W.fromHandle >>= \case
      Left err -> LTIO.hPutStrLn IO.stderr
        (TB.toLazyText (TB.fromString metric <> ": " <> showError err))
      Right whisper0 -> do
        let whisper1 = W.clean whisper0
        NSLB.sendAll sock (BB.toLazyByteString (toCarbonPlaintext metricBytes whisper1))
        LTIO.putStrLn (TB.toLazyText (TB.fromString metric <> ": Success"))

commandParser :: P.Parser Command
commandParser = P.hsubparser $ mconcat
  [ P.command "data" $ P.info
      (CommandData <$> cleanParser <*> pathParser)
      (P.progDesc "Dump the timestamp-metric data from a whisper file.")
  , P.command "header" $ P.info
      (CommandHeader <$> pathParser)
      (P.progDesc "Dump the metadata and archive information from a whisper file.")
  , P.command "send" $ P.info
      (CommandSend <$> directoryParser <*> hostParser)
      (P.progDesc "Send metrics from a database of whisper files to carbon.")
  , P.command "list" $ P.info
      (CommandList <$> directoryParser)
      (P.progDesc "List the names that graphite would use for the metrics in a database of whisper files.")
  ]

directoryParser :: P.Parser String
directoryParser = P.argument P.str $ mconcat
  [ P.metavar "DIR"
  , P.help "Directory of whisper files, paths used to determine metric names"
  ]

hostParser :: P.Parser String
hostParser = P.argument P.str $ mconcat
  [ P.metavar "CARBON"
  , P.help "Carbon host"
  ]

pathParser :: P.Parser String
pathParser = P.argument P.str $ mconcat
  [ P.metavar "FILE"
  , P.help "Path to the whisper file"
  ]

cleanParser :: P.Parser Bool
cleanParser = P.switch $ mconcat
  [ P.long "clean"
  , P.short 'c'
  , P.help "Remove metrics with invalid timestamps"
  ]

data Command
  = CommandData Bool String
  | CommandHeader String
  | CommandList String
  | CommandSend
      String -- directory
      String -- carbon host

showError :: W.WhisperError -> TB.Builder
showError = \case
  W.WhisperErrorMetadata ->
    "Encountered end of file while parsing 16 bytes of opening metadata."
  W.WhisperErrorAggregation w ->
    "Invalid aggregation type. This should be encoded as as number between " <>
    "1 and 8, but the number " <> TBI.decimal w <> " was given."
  W.WhisperErrorArchive ix ->
    "Encountered end of file while parsing the information of archive " <>
    TBI.decimal ix <> " in the header."
  W.WhisperErrorData ix ->
    "Encountered end of file while parsing the data of archive " <>
    TBI.decimal ix <> "."
  W.WhisperErrorLeftovers ->
    "Encountered additional bytes in file after end of the last archive." 

toCarbonPlaintext ::
     ByteString -- metric name
  -> W.Whisper
  -> BB.Builder
toCarbonPlaintext metric (W.Whisper _ archives) =
  foldMap
    ( \(W.Archive timestamps values) -> 
      let !szData = PM.sizeofPrimArray timestamps
          goB !ixData = if ixData < szData
            then
              let timestamp = PM.indexPrimArray timestamps ixData
                  value = PM.indexPrimArray values ixData
               in BB.byteString metric
               <> BB.char7 ' '
               <> BB.byteString (toFixed 12 value)
               <> BB.char7 ' '
               <> BB.word32Dec timestamp
               <> BB.char7 '\n'
               <> goB (ixData + 1)
            else mempty
       in goB 0
    ) archives

builderData :: W.Whisper -> TB.Builder
builderData (W.Whisper _ archives) = goA 0 where
  !szArchives = PM.sizeofArray archives
  goA !ixArchive = if ixArchive < szArchives
    then
      let !(W.Archive timestamps values) = PM.indexArray archives ixArchive
          !szData = PM.sizeofPrimArray timestamps
          goB !ixData = if ixData < szData
            then
              let timestamp = PM.indexPrimArray timestamps ixData
                  value = PM.indexPrimArray values ixData
               in TBI.decimal ixArchive
               <> TB.singleton ' '
               <> TBI.decimal ixData
               <> TB.singleton ' '
               <> TBI.decimal timestamp
               <> TB.singleton ' '
               <> TBRF.formatRealFloat TBRF.Fixed Nothing value
               <> TB.singleton '\n'
               <> goB (ixData + 1)
            else goA (ixArchive + 1)
       in goB 0
    else mempty

builderHeader :: W.Header -> TB.Builder
builderHeader (W.Header (W.Metadata agg maxRet factor archiveCount) infos) =
     "Metadata:\n  Aggregation Method: "
  <> TB.fromString (show agg)
  <> "\n  Maximum Retention: "
  <> TBI.decimal maxRet
  <> "\n  xFilesFactor: "
  <> TBRF.formatRealFloat TBRF.Fixed Nothing factor
  <> TB.singleton '\n'
  <> foldMap builderArchiveInfo infos
  

builderArchiveInfo :: W.ArchiveInfo -> TB.Builder
builderArchiveInfo (W.ArchiveInfo offset secondsPerPoint points) = 
     "\nArchive Info:\n  Offset: "
  <> TBI.decimal offset
  <> "\n  Seconds per Point: "
  <> TBI.decimal secondsPerPoint
  <> "\n  Points: "
  <> TBI.decimal points
  <> "\n  Retention: "
  <> TBI.decimal (secondsPerPoint * points)
  <> "\n  Size: "
  <> TBI.decimal (points * 12)
  <> TB.singleton '\n'

traverseWhisperDatabase_ :: FilePath -> ([String] -> IO a) -> IO ()
traverseWhisperDatabase_ root action = go [] where
  go subpath = do
    ds <- listDirectory (root ++ "/" ++ intercalate "/" (reverse subpath))
    forM_ ds $ \d -> do
      let subpath' = d : subpath
          proper = reverse subpath'
          filename = root ++ "/" ++ intercalate "/" proper
      isDir <- doesDirectoryExist filename
      if isDir
        then go subpath'
        else case stripSuffix ".wsp" d of
          Nothing -> pure ()
          Just d' -> action (reverse (d' : subpath)) $> ()

stripSuffix :: Eq a => [a] -> [a] -> Maybe [a]
stripSuffix a b = reverse <$> stripPrefix (reverse a) (reverse b)
