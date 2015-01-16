# execute_.py

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

"""Functions for command execution."""

from deso.cleanup import (
  defer,
)
from os import (
  O_RDWR,
  O_CLOEXEC,
  _exit,
  close as close_,
  devnull,
  dup2,
  execl,
  fork,
  open as open_,
  pipe2,
  read,
  waitpid,
  write,
)
from select import (
  PIPE_BUF,
  POLLERR,
  POLLHUP,
  POLLIN,
  POLLNVAL,
  POLLOUT,
  POLLPRI,
  poll,
)
from sys import (
  stderr,
  stdin,
  stdout,
)


def execute(*args, data_in=None, read_out=False, read_err=True):
  """Execute a program synchronously."""
  return pipeline([args], data_in, read_out, read_err)


def _pipeline(commands, fd_in, fd_out, fd_err):
  """Run a series of commands connected by their stdout/stdin."""
  pids = []
  first = True

  for i, command in enumerate(commands):
    last = i == len(commands) - 1

    # If there are more commands upcoming then we need to set up a pipe.
    if not last:
      fd_in_new, fd_out_new = pipe2(O_CLOEXEC)

    pids += [fork()]
    child = pids[-1] == 0

    if child:
      if not first:
        # Establish communication channel with previous process.
        dup2(fd_in_old, stdin.fileno())
        close_(fd_in_old)
        close_(fd_out_old)
      else:
        dup2(fd_in, stdin.fileno())

      if not last:
        # Establish communication channel with next process.
        close_(fd_in_new)
        dup2(fd_out_new, stdout.fileno())
        close_(fd_out_new)
      else:
        dup2(fd_out, stdout.fileno())

      # Stderr is redirected for all commands in the pipeline because each
      # process' output should be rerouted and stderr is not affected by
      # the pipe between the processes in any way.
      dup2(fd_err, stderr.fileno())

      execl(command[0], *command)
      # This statement should never be reached: either exec fails in
      # which case a Python exception should be raised or the program is
      # started in which case this process' image is overwritten anyway.
      # Keep it to be absolutely safe.
      _exit(-1)
    else:
      if not first:
        close_(fd_in_old)
        close_(fd_out_old)
      else:
        first = False

      # If there are further commands then update the "old" pipe file
      # descriptors for future reference.
      if not last:
        fd_in_old = fd_in_new
        fd_out_old = fd_out_new

  return pids


def formatPipeline(commands):
  """Convert a pipeline into a string."""
  return " | ".join(map(" ".join, commands))


def _wait(pids, commands, data_err):
  """Wait for all processes represented by a list of process IDs.

    Although it might not seem necessary to wait for any other than the
    last process, we wait for all of them. The main reason is that we
    want to clean up all left-over zombie processes.

    Notes:
      We also check the return code of every child process and raise an
      error in case one of them did not succeed. This behavior differs
      from that of bash, for instance, where no return code checking is
      performed for all but the last process in the chain. This approach
      is considered more safe in the face of failures. That is, unless
      there is some form of error checking being performed on the stream
      being passed through a pipe, there is no way for the last command
      to notice a failure of a previous command. As such, it might
      succeed although not the entire input/output was processed
      overall (because a previous command failed in an intermediate
      stage). We set a high priority on reporting potential failures to
      users.
  """
  assert len(pids) == len(commands)
  failed = None

  for i, pid in enumerate(pids):
    _, status = waitpid(pid, 0)

    if status != 0 and not failed:
      # Only remember the first failure here, then continue clean up.
      failed = formatPipeline([commands[i]])

  if failed:
    error = data_err.decode("utf-8") if data_err else None
    raise ChildProcessError(status, failed, error)


def _write(data):
  """Write data to one of our pipe dicts."""
  # Note that we are only guaranteed to write PIPE_BUF bytes at a time
  # without blocking.
  count = write(data["out"], data["data"][:PIPE_BUF])

  data["data"] = data["data"][count:]
  return not data["data"]


def _read(data):
  """Read data from one of our pipe dicts."""
  # We use 4 KiB as the maximum buffer size. This is quite a bit smaller
  # than the 64 KiB that /bin/cat apparently uses (and that seem to be
  # the default buffer size of pipes on some systems) but we expect way
  # less high-volume data to be read here (it should be piped directly
  # to the next process instead of going through a Python buffer). It
  # still is kind of an arbitrary value. We could also start of with a
  # small(er) value and increase it with every iteration or, if
  # performance measurements suggest it, just pick a larger value
  # altogether.
  buf = read(data["in"], 4 * 1024)
  if buf:
    data["data"] += buf
    return False
  else:
    return True


# The event mask for which to poll for a write channel (such as stdin).
_OUT = POLLOUT | POLLHUP | POLLERR
# The event mask for which to poll for a read channel (such as stdout).
_IN = POLLPRI | POLLHUP | POLLIN
# The event mask describing all error events on which we close the
# respective file descriptor.
_ERR = POLLHUP | POLLERR | POLLNVAL


class _PipelineFileDescriptors:
  """This class manages file descriptors for use with any pipeline of commands."""
  def __init__(self, later, here, data_in, read_out, read_err):
    """Initialize the pipe infrastructure on demand."""
    # We got two defer objects here. So here is how it works: Some of
    # the resources should be freed latest after the pipeline finished
    # its work. That is what 'here' is for. Others need to be freed
    # later (by 'later'), think, the file descriptors we need to poll.
    def pipeWrite(setup, data):
      """Conditionally setup a pipe for writing data."""
      if setup:
        data["in"], data["out"] = pipe2(O_CLOEXEC)
        data["close"] = later.defer(lambda: close_(data["out"]))
        here.defer(lambda: close_(data["in"]))

    def pipeRead(setup, data):
      """Conditionally setup a pipe for reading data."""
      if setup:
        data["in"], data["out"] = pipe2(O_CLOEXEC)
        data["close"] = later.defer(lambda: close_(data["in"]))
        here.defer(lambda: close_(data["out"]))

    # We need three dict objects, each representing one of the available
    # data channels. Depending on whether the channel is actually used
    # or not it gets populated on demand or stays empty, respectively.
    self._stdin = {}
    self._stdout = {}
    self._stderr = {}

    write_in = data_in is not None
    # Also store the data itself to be able to deliver it to stdin.
    if write_in:
      self._stdin["data"] = data_in

    # We want to redirect all file descriptors that we do not want
    # anything from to /dev/null. But we only want to open the latter in
    # case someone really requires it, i.e., if not all three channels
    # are connected to pipes anyway.
    # TODO: We need to be able to support simply keeping the file
    #       descriptors open as well.
    if not (write_in and read_out and read_err):
      self._null = open_(devnull, O_RDWR | O_CLOEXEC)
      here.defer(lambda: close_(self._null))

    # Now we need to set up the pipes on-demand.
    pipeWrite(write_in, self._stdin)
    pipeRead(read_out, self._stdout)
    pipeRead(read_err, self._stderr)


  def poll(self):
    """Poll the file pipe descriptors for more data until each indicated that it is done."""
    def pollWrite(data):
      """Conditionally set up polling for write events."""
      if data:
        poll_.register(data["out"], _OUT)
        data["unreg"] = d.defer(lambda: poll_.unregister(data["out"]))
        polls[data["out"]] = data

    def pollRead(data):
      """Conditionally set up polling for read events."""
      if data:
        data["data"] = bytearray()
        poll_.register(data["in"], _IN)
        data["unreg"] = d.defer(lambda: poll_.unregister(data["in"]))
        polls[data["in"]] = data

    # We need a poll object if we want to send any data to stdin or want
    # to receive any data from stdout or stderr.
    if self._stdin or self._stdout or self._stderr:
      poll_ = poll()

    # We use a dictionary here to elegantly look up the entry (which is,
    # another dictionary) for the respective file descriptor we received
    # an event for and to decide if we need to poll more.
    polls = {}

    with defer() as d:
      # Set up the polling infrastructure.
      pollWrite(self._stdin)
      pollRead(self._stdout)
      pollRead(self._stderr)

      while polls:
        events = poll_.poll()

        for fd, event in events:
          close = False
          data = polls[fd]

          # Note that reading (POLLIN or POLLPRI) and writing (POLLOUT)
          # are mutually exclusive operations on a pipe. All can be
          # combined with a HUP or with other errors (POLLERR or
          # POLLNVAL; even though we did not subscribe to them), though.
          if event & POLLOUT:
            close = _write(data)
          elif event & POLLIN or event & POLLPRI:
            if event & POLLHUP:
              # In case we received a combination of a data-is-available
              # and a HUP event we need to make sure that we flush the
              # entire pipe buffer before we stop the polling. Otherwise
              # we might leave data unread that was successfully sent to
              # us.
              # Note that from a logical point of view this problem
              # occurs only in the receive case. In the write case we
              # have full control over the file descriptor ourselves and
              # if the remote side closes its part there is no point in
              # sending any more data.
              while not _read(data):
                pass
            else:
              close = _read(data)

          # We explicitly (and early, compared to the defers we
          # scheduled previously) close the file descriptor on all error
          # events and POLLHUP, or when we received EOF (for reading) or
          # run out of data to send (for writing).
          if event & _ERR or close:
            data["close"]()
            data["unreg"]()
            del polls[fd]

      return self._stdout["data"] if self._stdout else b"",\
             self._stderr["data"] if self._stderr else b""


  def stdin(self):
    """Retrieve the stdin file descriptor ready to be handed to a process."""
    return self._stdin["in"] if self._stdin else self._null


  def stdout(self):
    """Retrieve the stdout file descriptor ready to be handed to a process."""
    return self._stdout["out"] if self._stdout else self._null


  def stderr(self):
    """Retrieve the stderr file descriptor ready to be handed to a process."""
    return self._stderr["out"] if self._stderr else self._null


def pipeline(commands, data_in=None, read_out=False, read_err=True):
  """Execute a pipeline, supplying the given data to stdin and reading from stdout & stderr."""
  with defer() as later:
    with defer() as here:
      # Set up the file descriptors to pass to our execution pipeline.
      # Whether or not to create pipes or use /dev/null depends on
      # whether we want to read/write to stdin, stdout, and stderr,
      # respectively.
      fds = _PipelineFileDescriptors(later, here, data_in, read_out, read_err)

      # Finally execute our pipeline and pass in the prepared file
      # descriptors to use.
      pids = _pipeline(commands, fds.stdin(), fds.stdout(), fds.stderr())

    data_out, data_err = fds.poll()

  # We have read or written all data that was available, the last thing
  # to do is to wait for all the processes to finish and to clean them
  # up.
  _wait(pids, commands, data_err)
  return data_out, data_err
