import System.IO
import Test.Tasty

import qualified Database.CQRS.InMemoryTest    as InMem
import qualified Database.CQRS.TabularDataTest as TabularData
import qualified Database.CQRS.TransformerTest as Transformer

main :: IO ()
main = do
  -- `bazel test` doesn't set LANG and co.
  hSetEncoding stdout utf8
  hSetEncoding stderr utf8
  defaultMain $ testGroup "All tests"
    [ InMem.tests
    , TabularData.tests
    , Transformer.tests
    ]
