module SSync.SimpleQueue where

data SimpleQueue a = SimpleQueue [a] [a] deriving (Show)

empty :: SimpleQueue a
empty = SimpleQueue [] []

enq :: SimpleQueue a -> a -> SimpleQueue a
enq (SimpleQueue front back) x = SimpleQueue front (x:back)

deq :: SimpleQueue a -> Maybe (SimpleQueue a, a)
deq (SimpleQueue (x:xs) back) = Just (SimpleQueue xs back, x)
deq (SimpleQueue [] back@(_:_)) = deq (SimpleQueue (reverse back) [])
deq _ = Nothing

q2list :: SimpleQueue a -> [a]
q2list (SimpleQueue front back) = front ++ reverse back
