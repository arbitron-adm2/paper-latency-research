"""Publication identity regressions use temporary, synthetic Git histories."""
import importlib.util
import os
from pathlib import Path
import subprocess
import tempfile
import unittest
from unittest.mock import patch

SPEC = importlib.util.spec_from_file_location('publish_guard', Path(__file__).resolve().parents[1] / 'tools/check_publish.py')
GUARD = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(GUARD)


class HistoryIdentityTests(unittest.TestCase):
    def setUp(self):
        self.folder = tempfile.TemporaryDirectory()
        self.addCleanup(self.folder.cleanup)
        self.root = Path(self.folder.name)
        subprocess.run(['git', 'init', '-q', str(self.root)], check=True)
        self.owner = 'synthetic-owner'

    def commit(self, author=None, committer=None):
        name = self.owner
        email = '123+' + self.owner + '@users.noreply.github.com'
        author = author or (name, email)
        committer = committer or (name, email)
        env = os.environ.copy()
        env.update(GIT_AUTHOR_NAME=author[0], GIT_AUTHOR_EMAIL=author[1],
                   GIT_COMMITTER_NAME=committer[0], GIT_COMMITTER_EMAIL=committer[1])
        subprocess.run(['git', '-C', str(self.root), '-c', 'commit.gpgsign=false', 'commit', '--allow-empty', '-qm', 'Synthetic fixture'], env=env, check=True)

    def check(self):
        with patch.object(GUARD, 'ROOT', self.root):
            return GUARD.check_history(self.owner)

    def test_expected_author_and_committer(self):
        self.commit()
        self.assertEqual(self.check(), {'commits': 1, 'findings': []})

    def test_foreign_committer_is_rejected_even_with_correct_author(self):
        self.commit(committer=('Other account', 'other@example.invalid'))
        result = self.check()
        self.assertEqual([r['role'] for r in result['findings']], ['committer'])

    def test_old_author_cannot_be_hidden_by_a_correct_new_head(self):
        self.commit(author=('Other account', 'other@example.invalid'))
        self.commit()
        result = self.check()
        self.assertEqual(result['commits'], 2)
        self.assertEqual([r['role'] for r in result['findings']], ['author'])


if __name__ == '__main__':
    unittest.main()
