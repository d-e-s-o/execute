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

from os import (
  _exit,
  close,
  devnull,
  dup2,
  execl,
  fork,
  pipe,
  read,
  waitpid,
)
from subprocess import (
  CalledProcessError,
)
from sys import (
  stderr,
  stdin,
  stdout,
)


def executeAndRead(*args):
  """Execute a command synchronously and retrieve its stdout output."""
  return pipelineAndRead([args])


def execute(*args):
  """Execute a program synchronously."""
  return pipeline([args])


def _pipeline(commands, fd_in, fd_out, fd_err):
  """Run a series of commands connected by their stdout/stdin."""
  first = True

  for i, command in enumerate(commands):
    last = i == len(commands) - 1

    # If there are more commands upcoming then we need to set up a pipe.
    if not last:
      fd_in_new, fd_out_new = pipe()

    pid = fork()
    child = pid == 0

    if child:
      if not first:
        # Establish communication channel with previous process.
        dup2(fd_in_old, stdin.fileno())
        close(fd_in_old)
        close(fd_out_old)
      else:
        dup2(fd_in, stdin.fileno())

      if not last:
        # Establish communication channel with next process.
        close(fd_in_new)
        dup2(fd_out_new, stdout.fileno())
        close(fd_out_new)
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
        close(fd_in_old)
        close(fd_out_old)
      else:
        first = False

      # If there are further commands then update the "old" pipe file
      # descriptors for future reference.
      if not last:
        fd_in_old = fd_in_new
        fd_out_old = fd_out_new

  return pid


def formatPipeline(commands):
  """Convert a pipeline into a string."""
  return " | ".join(map(" ".join, commands))


def pipeline(commands):
  """Execute a pipeline."""
  # TODO: It is questionable whether we should always silence stderr and
  #       stdout of the pipe by default. It surely makes sense for
  #       testing but for production it might be better to expose
  #       possible error messages.
  with open(devnull, "w+") as null:
    pid = _pipeline(commands, *[null.fileno()] * 3)

  _, status = waitpid(pid, 0)

  if status != 0:
    string = formatPipeline(commands)
    # TODO: Decide whether we want to use subprocess' CalledProcessError
    #       exception type here or have our own type and remove this
    #       dependency.
    raise CalledProcessError(status, string, None)


def pipelineAndRead(commands):
  """Execute a pipeline and read the output."""
  data = bytearray()
  fd_in, fd_out = pipe()

  with open(devnull, "w+") as null:
    pid = _pipeline(commands, null.fileno(), fd_out, null.fileno())

  close(fd_out)

  while True:
    # TODO: 256 is kind of an arbitrary value here. We could keep it,
    #       start of with a small(er) value and increase it with every
    #       iteration, or just pick a larger value altogether. Decide
    #       what the best approach is.
    buf = read(fd_in, 256)

    # Stop once we reached EOF.
    if not buf:
      break

    data += buf

  close(fd_in)
  _, status = waitpid(pid, 0)

  if status != 0:
    string = formatPipeline(commands)
    raise CalledProcessError(status, string, None)

  return data
