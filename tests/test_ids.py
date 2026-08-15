from __future__ import annotations

import unittest

from bourneprov.ids import new_ulid


class UlidTests(unittest.TestCase):
    def test_public_id_is_crockford_ulid_and_time_sortable(self) -> None:
        earlier = new_ulid(timestamp_ms=1_000)
        later = new_ulid(timestamp_ms=1_001)

        self.assertEqual(len(earlier), 26)
        self.assertTrue(set(earlier) <= set("0123456789ABCDEFGHJKMNPQRSTVWXYZ"))
        self.assertLess(earlier, later)


if __name__ == "__main__":
    unittest.main()
