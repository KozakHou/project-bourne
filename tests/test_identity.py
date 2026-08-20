from __future__ import annotations

import os
import unittest
from unittest.mock import patch

from bourneprov.identity import ProcessIdentity, current_process_identity


class ProcessIdentityTests(unittest.TestCase):
    @unittest.skipUnless(os.name == "posix", "POSIX effective identity test")
    def test_environment_username_changes_cannot_spoof_effective_identity(self) -> None:
        environment = {
            "USER": "bob",
            "LOGNAME": "bob",
            "LNAME": "bob",
            "USERNAME": "bob",
        }
        with (
            patch("bourneprov.identity.os.geteuid", return_value=1001),
            patch("bourneprov.identity._username_for_uid", return_value="alice"),
        ):
            original = current_process_identity()
            with patch.dict(os.environ, environment, clear=False):
                changed = current_process_identity()

        expected = ProcessIdentity(
            username="alice",
            effective_uid=1001,
            source="posix_effective_uid_password_database",
        )
        self.assertEqual(original, expected)
        self.assertEqual(changed, expected)

    def test_non_posix_fallback_is_explicit(self) -> None:
        with (
            patch("bourneprov.identity.os.name", "nt"),
            patch("bourneprov.identity.getpass.getuser", return_value="fallback-user"),
        ):
            identity = current_process_identity()

        self.assertEqual(identity.username, "fallback-user")
        self.assertIsNone(identity.effective_uid)
        self.assertEqual(identity.source, "platform_username_fallback")


if __name__ == "__main__":
    unittest.main()
