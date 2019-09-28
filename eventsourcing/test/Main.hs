import Test.Tasty

import qualified Database.CQRS.InMemoryTest as InMem

main :: IO ()
main = defaultMain $ testGroup "All tests" [InMem.tests]
