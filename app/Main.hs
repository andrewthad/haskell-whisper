{-# language LambdaCase #-}
{-# language OverloadedStrings #-}

import Options.Applicative ((<**>))
import System.Exit (exitFailure)

import qualified Data.Whisper as W
import qualified Options.Applicative as P
import qualified System.IO as IO
import qualified Data.Text.Lazy.IO as LTIO
import qualified Data.Text.Lazy.Builder as TB
import qualified Data.Text.Lazy.Builder.RealFloat as TBRF
import qualified Data.Text.Lazy.Builder.Int as TBI

main :: IO ()
main = do
  cmd <- P.execParser $ P.info
    (commandParser <**> P.helper)
    P.fullDesc
  case cmd of
    CommandHeader path -> runHeader path
      

runHeader :: String -> IO ()
runHeader path = do
  e <- IO.withFile path IO.ReadMode W.headerFromHandle
  case e of
    Left err -> do
      IO.hPutStrLn IO.stderr (showError err)
      exitFailure
    Right header -> LTIO.putStr (TB.toLazyText (builderHeader header))

commandParser :: P.Parser Command
commandParser = P.hsubparser $ mconcat
  [ P.command "data" $ P.info
      (CommandData <$> pathParser)
      (P.progDesc "Dump the timestamp-metric data from a whisper file.")
  , P.command "header" $ P.info
      (CommandHeader <$> pathParser)
      (P.progDesc "Dump the metadata and archive information from a whisper file.")
  ]

pathParser :: P.Parser String
pathParser = P.argument P.str $ mconcat
  [ P.metavar "file"
  , P.help "Path to the whisper file"
  ]

data Command
  = CommandData String
  | CommandHeader String

showError :: W.WhisperError -> String
showError = \case
  W.WhisperErrorMetadata ->
    "Encountered end of file while parsing 16 bytes of opening metadata."
  W.WhisperErrorAggregation w ->
    "Invalid aggregation type. This should be encoded as as number between " ++
    "1 and 8, but the number " ++ show w ++ " was given."
  W.WhisperErrorArchive ix ->
    "Encountered end of file while parsing the information of archive " ++
    show ix ++ " in the header."
  W.WhisperErrorData ix ->
    "Encountered end of file while parsing the data of archive " ++
    show ix ++ "."
  W.WhisperErrorLeftovers ->
    "Encountered additional bytes in file after end of the last archive." 

builderHeader :: W.Header -> TB.Builder
builderHeader (W.Header (W.Metadata agg maxRet factor archiveCount) infos) =
     "Metadata:\n  Aggregation Method: "
  <> TB.fromString (show agg)
  <> "\n  Maximum Retention: "
  <> TBI.decimal maxRet
  <> "\n  xFilesFactor: "
  <> TBRF.realFloat factor
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

