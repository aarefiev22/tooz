# -*- coding: utf-8 -*-

#    Copyright (C) 2013 Rackspace Hosting Inc. All Rights Reserved.
#    Copyright (C) 2013 Yahoo! Inc. All Rights Reserved.
#
#    Licensed under the Apache License, Version 2.0 (the "License"); you may
#    not use this file except in compliance with the License. You may obtain
#    a copy of the License at
#
#         http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
#    WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
#    License for the specific language governing permissions and limitations
#    under the License.

import abc
import collections
import enum
import logging
import time

from oslo_utils import timeutils
from oslo_utils import uuidutils
import six

import tooz


class JobState(enum.Enum):
    """Job states.

    Valid (implicit or explict) transitions are the following::

        +--------------+------------+-----------+----------+---------+
        |    Start     |   Event    |    End    | On Enter | On Exit |
        +--------------+------------+-----------+----------+---------+
        |   CLAIMED    | on_abandon | UNCLAIMED |    .     |    .    |
        |   CLAIMED    |  on_done   |  COMPLETE |    .     |    .    |
        | COMPLETE[$]  |     .      |     .     |    .     |    .    |
        | UNCLAIMED[^] |  on_claim  |  CLAIMED  |    .     |    .    |
        +--------------+------------+-----------+----------+---------+
    """

    #: Job has been claimed.
    CLAIMED = 'CLAIMED'

    #: Job has been completed (terminal state).
    COMPLETE = 'COMPLETE'

    #: Job is new or previously claimed (and then unclaimed); initial state.
    UNCLAIMED = 'UNCLAIMED'

    @classmethod
    def convert(cls, value):
        """Converts a string value into a enum value."""
        if isinstance(value, cls):
            return value
        if not isinstance(value, six.string_types):
            raise TypeError("String type expected,"
                            " not %s" % type(value).__name__)
        return cls(value.upper())


LOG = logging.getLogger(__name__)


def _generate_delays(delay, max_delay, multiplier=2):
    """Generator/iterator that provides back delays values.

    The values it generates increments by a given multiple after each
    iteration (using the max delay as a upper bound). Negative values
    will never be generated... and it will iterate forever (ie it will never
    stop generating values).
    """
    if max_delay < 0:
        raise ValueError("Provided delay (max) must be greater"
                         " than or equal to zero")
    if delay < 0:
        raise ValueError("Provided delay must start off greater"
                         " than or equal to zero")
    if multiplier < 1.0:
        raise ValueError("Provided multiplier must be greater than"
                         " or equal to 1.0")

    def _gen_it():
        # NOTE(harlowja): Generation is delayed so that validation
        # can happen before generation/iteration... (instead of
        # during generation/iteration)
        curr_delay = delay
        while True:
            curr_delay = max(0, min(max_delay, curr_delay))
            yield curr_delay
            curr_delay = curr_delay * multiplier

    return _gen_it()


class JobFailure(tooz.ToozError):
    """Failures related to jobs **must** derive from this base class."""


class JobNotFound(JobFailure):
    """Jobs that were expected to be found but were not (for some reason)."""


@six.add_metaclass(abc.ABCMeta)
class Job(object):
    """A abstraction that represents a named and trackable unit of work.

    A job connects a owner, a name, last modified and created on dates and
    any associated state that the job has (which must be enough to
    describe what the job is about).

    Only one entity will be allowed to own and operate on the job at
    a given time (for the foreseeable future).
    """

    def __init__(self, board, name, uuid=None, details=None):
        if uuid:
            self._uuid = uuid
        else:
            self._uuid = uuidutils.generate_uuid()
        self._name = name
        if not details:
            details = {}
        self._details = details
        self._board = board
        self._state = JobState.UNCLAIMED

    def wait(self, timeout=None,
             delay=0.01, delay_multiplier=2.0, max_delay=60.0,
             sleep_func=time.sleep):
        """Wait for job to enter completion state.

        If the job has not completed in the given timeout, then return false,
        otherwise return true (a job failure exception may also be raised if
        the job information can not be read, for whatever reason). Periodic
        state checks will happen every ``delay`` seconds where ``delay`` will
        be multipled by the given multipler after a state is found that is
        **not** complete.

        Note that if no timeout is given this is equivalent to blocking
        until the job has completed. Also note that if a jobboard backend
        can optimize this method then its implementation may not use
        delays (and backoffs) at all. In general though no matter what
        optimizations are applied implementations must **always** respect
        the given timeout value.
        """
        if timeout is not None:
            w = timeutils.StopWatch(duration=timeout)
            w.start()
        else:
            w = None
        delay_gen = _generate_delays(delay, max_delay,
                                     multiplier=delay_multiplier)
        while True:
            if w is not None and w.expired():
                return False
            if self.state == JobState.COMPLETE:
                return True
            sleepy_secs = six.next(delay_gen)
            if w is not None:
                sleepy_secs = min(w.leftover(), sleepy_secs)
            sleep_func(sleepy_secs)
        return False

    @abc.abstractproperty
    def last_modified(self):
        """The ``datetime.datetime`` the job was last modified."""

    @abc.abstractproperty
    def created_on(self):
        """The ``datetime.datetime`` the job was created on."""

    @property
    def state(self):
        """Access the current state of this job.

        Due to how distributed systems typically work a entity that has
        claimed a job **must** periodically check this state to ensure
        that it still actively owns the claim on this job since it is
        possible to have lost a claim on a job at any time. Once a claim
        that was previously owned has been lost it is then up to the
        application to correctly handle what to do to react to this
        situtation (typically stopping all further work is one
        mechanism).
        """
        return self._state 

    @property
    def uuid(self):
        """The uniquely identifying uuid of this job."""
        return self._uuid

    @property
    def details(self):
        """A dictionary of any details associated with this job.

        The details **must** provide enough information for consumers to know
        what to do with the job (and/or how to perform it).
        """
        return self._details

    @property
    def name(self):
        """The non-uniquely identifying name of this job."""
        return self._name

    def __str__(self):
        """Pretty formats the job into something *more* meaningful."""
        return "%s: %s (uuid=%s, details=%s)" % (type(self).__name__,
                                                 self.name, self.uuid,
                                                 self.details)


class JobBoardIterator(six.Iterator):
    """Iterator over a jobboard that iterates over potential jobs.

    It provides the following attributes:

    * ``only_unclaimed``: boolean that indicates whether to only iterate
      over unclaimed jobs
    * ``ensure_fresh``: boolean that requests that during every fetch of a new
      set of jobs this will cause the iterator to force the backend to
      refresh (ensuring that the board has the most recent job listings)
    * ``board``: the board this iterator was created from
    """

    _UNCLAIMED_JOB_STATES = (JobState.UNCLAIMED,)
    _JOB_STATES = tuple(JobState)

    def __init__(self, board,
                 board_fetch_func=None, board_removal_func=None,
                 only_unclaimed=False, ensure_fresh=False):
        self._board = board
        self._board_removal_func = board_removal_func
        self._board_fetch_func = board_fetch_func
        self._fetched = False
        self._jobs = collections.deque()
        self.only_unclaimed = only_unclaimed
        self.ensure_fresh = ensure_fresh

    @property
    def board(self):
        """The board this iterator was created from."""
        return self._board

    def __iter__(self):
        return self

    def _next_job(self):
        if self.only_unclaimed:
            allowed_states = self._UNCLAIMED_JOB_STATES
        else:
            allowed_states = self._JOB_STATES
        job = None
        while self._jobs and job is None:
            maybe_job = self._jobs.popleft()
            try:
                if maybe_job.state in allowed_states:
                    job = maybe_job
            except JobNotFound:
                if self._board_removal_func is not None:
                    self._board_removal_func(maybe_job)
            except JobFailure:
                LOG.warn("Failed determining the state of"
                         " job '%s'", maybe_job, exc_info=True)
        return job

    def __next__(self):
        if not self._jobs:
            if not self._fetched:
                if self._board_fetch_func is not None:
                    self._jobs.extend(
                        self._board_fetch_func(
                            ensure_fresh=self.ensure_fresh))
                self._fetched = True
        job = self._next_job()
        if job is None:
            raise StopIteration
        else:
            return job


@six.add_metaclass(abc.ABCMeta)
class JobBoard(object):
    """A place where jobs can be posted, reposted, claimed and transferred.

    There can be multiple implementations of this board, depending on the
    desired semantics and capabilities of the underlying board implementation.

    The name is meant to be an analogous to a board/posting
    system that is used in newspapers, or elsewhere to solicit jobs that
    people can interview and apply for (and then work on & complete).
    """

    def __init__(self, name):
        self._name = name

    @abc.abstractmethod
    def iterjobs(self, only_unclaimed=False, ensure_fresh=False):
        """Returns an iterator of jobs that are currently on this board.

        The ordering of this iteration **should** be by posting
        order (oldest to newest) if possible, but it is left up to the backing
        implementation to provide the order that best suits it (so don't depend
        on it always being oldest to newest).

        The iterator that is returned may support other attributes which can
        be used to further customize how iteration can be accomplished; read
        over with the backends iterator objects documentation to determine
        what other attributes are supported.

        :param only_unclaimed: boolean that indicates whether to only iteration
            over unclaimed jobs.
        :param ensure_fresh: boolean that requests to only iterate over the
            most recent jobs available, where the definition of what is recent
            is backend specific; it is allowable that a backend may ignore this
            value if the backends internal semantics/capabilities can not
            support this argument.
        """

    @abc.abstractmethod
    def wait(self, timeout=None):
        """Waits a given amount of time for jobs to be posted.

        When jobs are found then an iterator will be returned that can be used
        to iterate over those jobs.

        Since a board can be mutated on by multiple external entities at
        the **same** time the iterator that can be returned **may** still be
        empty due to other entities removing those jobs after the iterator
        has been created (be aware of this when using it).

        :param timeout: float that indicates how long to wait for a job to
            appear (if ``None`` then this may wait forever).
        """

    @abc.abstractproperty
    def job_count(self):
        """Returns how many jobs are on this board.

        This count may change as jobs appear or are removed so
        the accuracy of this count should not be used in a way that requires
        it to be exact & absolute (because it may not be).
        """

    @abc.abstractmethod
    def find_owner(self, job):
        """Gets the owner of the job if one exists."""

    @property
    def name(self):
        """The non-uniquely identifying name of this board."""
        return self._name

    @abc.abstractmethod
    def consume(self, job, who):
        """Permanently (and **atomically**) removes a job from the board.

        Consumption signals to the board (and any others examining the board)
        that this job has been completed by the entity that previously claimed
        that job.

        Only the entity that has claimed that job is able to consume the job.

        A job that has been consumed can not be reclaimed or reposted by
        another entity (job postings are immutable). Any entity consuming
        a unclaimed job (or a job they do not have a claim on) will cause an
        exception.

        This operation must cause all other entities connected to the
        backing system to no longer be able to claim that job (although they
        may be able to **attempt** to do so for a given amount of time due to
        eventually consistent views of the backing systems). Any such attempts
        should fail if tried (because at the time of claim it must
        fail).

        :param job: a job on the board that can be consumed (if it does
            not exist then a :py:class:`.JobNotFound` exception will
            be raised).
        :param who: string that names the entity performing the consumption,
            this must be the same name that was used for claiming this job.
        """

    @abc.abstractmethod
    def post(self, name, details=None):
        """**Atomically** creates and posts a job to the jobboard.

        This posting allowing others to attempt to claim that job (and
        subsequently work on that job). The contents of the provided details
        dictionary, or name (or a mix of these) must provide **enough**
        information for consumers to reference to construct and perform that
        jobs contained work (whatever it may be).

        Once a job has been posted it can only be removed by consuming that
        job (after that job is claimed). Any entity can post/propose jobs
        to the board (in the future this may be restricted).

        Returns a :py:class:`.Job` object representing the
        information that was posted.
        """

    @abc.abstractmethod
    def claim(self, job, who):
        """**Atomically** attempts to claim the provided job.

        If a job is claimed it is expected that the entity that claims that job
        will at sometime in the future work on that jobs contents and either
        fail at completing them (resulting in a reposting) or consume that job
        from the board (signaling its completion). If claiming fails then
        a corresponding exception will be raised to signal this to the claim
        attempter.

        Once a job has been claimed **no** other entity is allowed to also
        claim that job (until either the job has been consumed, or the owner
        has been determined to be dead, at which it is allowable for the claim
        to auto-release); auto-release is backend specific so read over the
        backend documentation to determine if auto-release is or is not
        supported.

        :param job: a job on the board that can be claimed (if it does
            not exist then a :py:class:`.JobNotFound` exception will
            be raised).
        :param who: string that names the claiming entity.
        """

    @abc.abstractmethod
    def abandon(self, job, who):
        """**Atomically** attempts to abandon the provided job.

        This abandonment signals to others that the job may now be reclaimed.
        This would typically occur if the entity that has claimed the job has
        failed or is unable to complete the job or jobs it had previously
        claimed.

        Only the entity that has claimed that job can abandon a job. Any entity
        abandoning a unclaimed job (or a job they do not own) will cause an
        exception.

        :param job: a job on the bboard that can be abandoned (if it does
            not exist then a :py:class:`.JobNotFound` exception will be
            raised).
        :param who: string that names the entity performing the abandoning,
            this must be the same name that was used for claiming this job.
        """

    @abc.abstractmethod
    def trash(self, job, who):
        """Trash the provided job.

        Trashing a job signals to others that the job is broken and should not
        be reclaimed. This is provided as an option for users to be able to
        remove jobs from the board externally. The trashed job details should
        be kept around in an alternate location to be reviewed, if desired.

        Only the entity that has claimed that job can trash a job. Any entity
        trashing a unclaimed job (or a job they do not own) will cause an
        exception.

        :param job: a job on the board that can be trashed (if it does
            not exist then a :py:class:`.JobNotFound` exception will
            be raised).
        :param who: string that names the entity performing the trashing,
            this must be the same name that was used for claiming this job.
        """

    @abc.abstractproperty
    def connected(self):
        """Returns if this board is connected."""

    @abc.abstractmethod
    def connect(self):
        """Opens the connection to any backend system."""

    @abc.abstractmethod
    def close(self):
        """Close the connection to any backend system.

        Once closed the board can no longer be used (unless reconnection
        occurs).

        This must release all currently claimed jobs (either by taking
        advantage of auto-release or forcefully abandoining any current
        claims).
        """
