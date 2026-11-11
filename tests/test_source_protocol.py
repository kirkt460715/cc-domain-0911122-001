import unittest
from lab.http import serving
from lab.provider import Provider
from lab.demo import request
class SourceProtocol(unittest.TestCase):
    def test_source_roundtrip(self):
        with serving(Provider()) as base:
            result=request(base,'/view',None)
            self.assertIsNotNone(result)

    def test_summary_matches_bundle_and_expiry_is_visible(self):
        from registry_bridge.feeds.bundle import verify_bundle
        from registry_bridge.feeds.message import digest
        import urllib.error
        with serving(Provider()) as base:
            suffix='?epoch=exchange-1&revision=1&lower=4&upper=7'
            bundle=request(base,'/bundle'+suffix);messages=verify_bundle(bundle)
            summary=request(base,'/summary'+suffix)
            self.assertEqual(summary['sha256'],digest([[x.sequence,x.fingerprint] for x in messages]))
            request(base,'/control',{'expire':True})
            with self.assertRaises(urllib.error.HTTPError) as failure:request(base,'/summary'+suffix)
            self.assertEqual(failure.exception.code,410)
