module TSBQueue (TSBQueue, newTSBQueueIO, writeTSBQueue, readTSBQueue, drainTSBQueue) where

import Control.Concurrent.STM
import Control.Monad (unless)
import Control.Applicative ((<$>), (<|>))

data TSBQueue a = TSBQueue !Int (TVar Int) (a -> Int) (TQueue (a, Int))
newTSBQueueIO :: Int -> (a -> Int) -> IO (TSBQueue a)
newTSBQueueIO maxTotalSize sizer = do
  underlying <- newTQueueIO
  current <- newTVarIO 0
  return $ TSBQueue maxTotalSize current sizer underlying

writeTSBQueue :: TSBQueue a -> a -> STM ()
writeTSBQueue (TSBQueue limit current sizer underlying) a = do
  c <- readTVar current
  let size = sizer a
      new = c + size
  unless (new <= limit) retry
  writeTVar current new
  writeTQueue underlying (a, size)

readTSBQueue :: TSBQueue a -> STM a
readTSBQueue (TSBQueue _ current _ underlying) = do
  (res, size) <- readTQueue underlying
  modifyTVar current (subtract size)
  return res

drainTSBQueue :: TSBQueue a -> STM ()
drainTSBQueue q = do
  e <- (Just <$> readTSBQueue q) <|> return Nothing
  case e of
   Just _ -> drainTSBQueue q
   Nothing -> return ()
