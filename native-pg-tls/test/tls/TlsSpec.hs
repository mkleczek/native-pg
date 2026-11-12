{-# LANGUAGE OverloadedStrings #-}

module TlsSpec (spec) where

import Database.PostgreSQL.Tls.Internal
import Database.PostgreSQL.Transport (RootCertificates (..), ServerVerification (..), TlsError (..), TlsSettings (..))
import Skeletest
import qualified Skeletest.Predicate as P

spec :: Spec
spec = do
  describe "a configuration" $
    it "needs its root certificate file" $
      newTlsConfig (settings (VerifyFull (RootCertificateFile (certificate "missing.crt")))) "localhost"
        `shouldSatisfy` P.throws (P.con (TlsError (P.hasInfix "could not read root certificate file")))

-- | The settings with the verification, and rustls's defaults otherwise.
settings :: ServerVerification -> TlsSettings
settings verification = TlsSettings verification Nothing [] [minBound .. maxBound] True False

certificate :: FilePath -> FilePath
certificate name = "test/certificates/" <> name
