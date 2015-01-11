# testExecute.py

#/***************************************************************************
# *   Copyright (C) 2014-2015 Daniel Mueller (deso@posteo.net)              *
# *                                                                         *
# *   This program is free software: you can redistribute it and/or modify  *
# *   it under the terms of the GNU General Public License as published by  *
# *   the Free Software Foundation, either version 3 of the License, or     *
# *   (at your option) any later version.                                   *
# *                                                                         *
# *   This program is distributed in the hope that it will be useful,       *
# *   but WITHOUT ANY WARRANTY; without even the implied warranty of        *
# *   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the         *
# *   GNU General Public License for more details.                          *
# *                                                                         *
# *   You should have received a copy of the GNU General Public License     *
# *   along with this program.  If not, see <http://www.gnu.org/licenses/>. *
# ***************************************************************************/

"""Test command execution wrappers."""

from deso.execute import (
  execute as execute_,
  findCommand,
  formatPipeline,
  pipeline as pipeline_,
)
from os import (
  remove,
)
from os.path import (
  isfile,
)
from tempfile import (
  mktemp,
)
from unittest import (
  TestCase,
  main,
)


_TRUE = findCommand("true")
_FALSE = findCommand("false")
_ECHO = findCommand("echo")
_TOUCH = findCommand("touch")
_CAT = findCommand("cat")
_TR = findCommand("tr")
_DD = findCommand("dd")


def execute(*args, data_in=None, read_out=False, read_err=False):
  """Run a program with reading from stderr disabled by default."""
  return execute_(*args, data_in=data_in,
                         read_out=read_out,
                         read_err=read_err)


def pipeline(commands, data_in=None, read_out=False, read_err=False):
  """Run a pipeline with reading from stderr disabled by default."""
  return pipeline_(commands, data_in=data_in,
                             read_out=read_out,
                             read_err=read_err)


class TestExecute(TestCase):
  """A test case for command execution functionality."""
  def testExecuteAndNoOutput(self):
    """Test command execution and output retrieval for empty output."""
    output, _ = execute(_TRUE, read_out=True)
    self.assertEqual(output, b"")


  def testExecuteAndOutput(self):
    """Test command execution and output retrieval."""
    output, _ = execute(_ECHO, "success", read_out=True)
    self.assertEqual(output, b"success\n")


  def testExecuteAndOutputMultipleLines(self):
    """Test command execution with multiple lines of output."""
    string = "first-line\nsuccess"
    output, _ = execute(_ECHO, string, read_out=True)
    self.assertEqual(output, bytes(string + "\n", "utf-8"))


  def testExecuteAndInputOutput(self):
    """Test that we can redirect stdin and stdout at the same time."""
    output, _ = execute(_CAT, data_in=b"success", read_out=True)
    self.assertEqual(output, b"success")


  def testExecuteRedirectAll(self):
    """Test that we can redirect stdin, stdout, and stderr at the same time."""
    out, err = execute(_DD, data_in=b"success", read_out=True, read_err=True)
    line1, line2, _ = err.decode("utf-8").splitlines()

    self.assertEqual(out, b"success")
    self.assertTrue(line1.endswith("records in"))
    self.assertTrue(line2.endswith("records out"))


  def testExecuteThrowsOnCommandFailure(self):
    """Verify that a failing command causes an exception to be raised."""
    with self.assertRaises(ChildProcessError):
      execute(_FALSE)


  def testExecuteThrowsAndReportsError(self):
    """Verify that a failing command's exception contains the stderr output."""
    path = mktemp()
    regex = r"No such file or directory"

    with self.assertRaises(AssertionError):
      # This command should fail the assertion because reading from
      # standard error is not turned on and as a result the error message
      # printed on stderr will not be contained in the ChildProcessError
      # error message.
      with self.assertRaisesRegex(ChildProcessError, regex):
        execute(_CAT, path)

    # When reading from stderr is turned on the exception must contain
    # the above phrase.
    with self.assertRaisesRegex(ChildProcessError, regex):
      execute(_CAT, path, read_err=True)


  def testPipelineThrowsForFirstFailure(self):
    """Verify that if some commands fail in a pipeline, the error of the first is reported."""
    for cmd in [_FALSE, _TRUE]:
      # Note that mktemp does not create the file, it just returns a
      # file name unique at the time of the invocation.
      path = mktemp()
      regex = r"No such file or directory"

      # A pipeline of commands with one or two of them failing.
      commands = [
        [_ECHO, "test"],
        [_CAT, path],
        [cmd],
      ]

      with self.assertRaisesRegex(ChildProcessError, regex):
        pipeline(commands, read_err=True)


  def testFormatPipeline(self):
    """Test conversion of a series of commands into a string."""
    commands = [
      [_ECHO, "test"],
      [_TR, "t", "z"],
      [_TR, "z", "t"],
    ]
    expected = "{echo} test | {tr} t z | {tr} z t".format(echo=_ECHO, tr=_TR)

    self.assertEqual(formatPipeline(commands), expected)


  def testPipelineSingleProgram(self):
    """Verify that a pipeline can run a single program."""
    path = mktemp()
    commands = [[_TOUCH, path]]

    self.assertFalse(isfile(path))
    pipeline(commands)
    self.assertTrue(isfile(path))

    remove(path)


  def testPipelineMultiplePrograms(self):
    """Verify that a pipeline can run two and more programs."""
    def doTest(intermediates):
      """Actually perform the test for a given pipeline depth."""
      tmp_file = mktemp()
      identity = [_TR, "a", "a"]
      commands = [
        [_ECHO, "test-abc-123"],
        [_DD, "of=%s" % tmp_file],
      ]

      commands[1:1] = [identity] * intermediates

      self.assertFalse(isfile(tmp_file))
      pipeline(commands)
      self.assertTrue(isfile(tmp_file))

      remove(tmp_file)

    for i in range(0, 4):
      doTest(i)


  def testPipelineWithRead(self):
    """Test execution of a pipeline and reading the output."""
    commands = [
      [_ECHO, "suaaerr"],
      [_TR, "a", "c"],
      [_TR, "r", "s"],
    ]
    output, _ = pipeline(commands, read_out=True)

    self.assertEqual(output, b"success\n")


  def testPipelineWithExcessiveRead(self):
    """Verify that we do not deadlock when receiving large quantities of data."""
    megabyte = 1024 * 1024
    megabytes = 8
    commands = []
    data = b"a" * megabytes * megabyte

    # Test with 1 to 3 programs in the pipeline.
    for _ in range(3):
      commands += [[_DD]]
      # TODO: The following does not work and fails with "[Errno 32]
      #       Broken Pipe". Find out why. Some people suggest it might
      #       be a problem on the Python side. Need to understand in
      #       either case. Note that adding 'iflag=fullblock' to the dd
      #       command solves the issue.
      #commands += [[_DD, 'bs=%s' % megabyte, 'count=%s' % megabytes]]

      out, _ = pipeline(commands, data_in=data, read_out=True)
      self.assertEqual(len(out), len(data))


  def testPipelineWithFailingCommand(self):
    """Verify that a failing command in a pipeline fails the entire execution."""
    identity = [_TR, "a", "a"]
    commands = [
      [_ECHO, "test-abc-123"],
      identity,
      [_FALSE],
    ]

    # Run twice, once with failing command at the end and once somewhere
    # in the middle.
    for _ in range(2):
      with self.assertRaises(ChildProcessError):
        pipeline(commands)

      commands += [identity]


if __name__ == "__main__":
  main()
