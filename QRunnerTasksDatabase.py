#!/usr/bin/env python3

import csv, re, sys, os

from pathlib import Path

'''
Implements a simple tasks database with a CSV file.

Scales reasonably well up to a few hundred thousand tasks.

Beyond that, things get slow reading and writing the file.
'''

DEFAULT_TASKS_FILE_TEXT = '''\
# States:
#
# '', 0, IGNORE: ignore, skip over
# 1, NEW: added, not yet running
# 2: LAUNCHING: qrunner is about to launch the task
# 3: RUNNING: task is running, PID field is valid
# 3: FINISHED: task has finished, RC field is valid
# 4: ARCHIVING: task has finished, input/output files and RC are being archived
# 5: DELETE: task has finished, input/output files are ready to be deleted
# 6: DELETED: task has finished, ready to delete this entry (same as 0/IGNORE)
# -1: INVALID: generic error; task entry is invalid
# -2: FAILED: qrunner was unable to launch the task
# -3: DIED: qrunner was not running when the the task finished
# -4: ZOMBIE: task is still running and cannot be SIGKILLED, PID field is valid
# -5: EXCEPTION: qrunner had an internal error; see the exception field
# -6: KILLING: qrunning tried to kill the task and is waiting
# -7: KILLING9: qrunning is trying to SIGKIILL the task and is waiting
# -8: KILLED: qrunner killed the task, RC field is valid
# -9: KILLED9: qrunner SIGKILL'd the task, RC field is not valid
# -10: LOST: qrunner stopped running and the task is still running
# Anything else: Invalid (same as -1/INVALID)

# RC:
#
# '': blank string - state is not in 3, -3, or -4 yet
# 0-255: normal process return codes

# Input/output files:
#
# If the input field is blank, then stdin will simply be /dev/null
# If the output or stderr field is blank, then the output will be discarded
# If the filename is just -, then it will be based on a pattern of
# group-task-comment-user-host-command-pid-in/out/err.txt

# Groups starting with a "#" (including leading spaces) will be ignored.
# If the group is blank or 0 or less than zero, then any tasks in that group
# can be run at any time and do not depend on other groups finishing first.
# All the tasks in the same group can run in parallel.
# All the tasks in a given group need to be run before any tasks in a higher-
# numbered group may run (other than 0 or less than zero or blank).
# The comment field is ignored, but can be used to help keep track of the file-
# names generated.
# The host indicates where the job is running. If blank, the local host is
# assumed.
# The user indicates the user the job should run is. If blank, then the user of
# qrunner will be used.
# The pwd indicates the working directory where the job will be run. Input and
# output filenames will be assumed to be based off of this directory. If empty
# the user's home directory will be used.
# The command is the actual process that will be run. If blank, then the shell
# will be assumed and the arguments will be passed directly to the shell as a
# shell command.
# When the task is running the PID field will be set. The PID field is only
# valid when in the 2/RUNNING state or -4/ZOMBIE state.
# If fields at the end are missing, they will be treated as if they are blank
# If the task runner has an internal error, it will go into the EXCEPTION

'''

ROWNUM_KEY = 'rownum'
FUNCTION_KEY = 'function'

class QRunnerTasksDatabase:

    def parse(self):
        standard_headers = ['comment','status','pid','rc','command','group',
                            'user','host','pwd','inputfile','outputfile','errorfile','exception'
                            ]

        self.rawdata = []
        self.rawdata_len = 0

        if self.tasksdb_text is None:
            self.preservetext = DEFAULT_TASKS_FILE_TEXT
            self.headers = standard_headers
            return

        '''Find lines that are blank, start with a comment, or a space, and preserve them'''
        self.preservetext = ''
        csvs = []
        for l in re.split(r"\r\n|\r|\n", self.tasksdb_text):
            if re.match(r"^$|^\s|^#", l):
                self.preservetext += l;
                self.preservetext += '\n';
            else:
                csvs.append(l)

        self.preservetext = self.preservetext[:-1]

        if len(csvs) > 0:
            self.headers = list(csv.reader([csvs[0]]))[0]
            # If the first CSV line doesn't start with "comment", we probably don't have a header line.
            if self.headers[0].lower() == 'comment':
                if ROWNUM_KEY in self.headers:
                    raise Exception("`rownum' is not a valid field in the CSV file. Please remove it.")
                csvs_to_add = csv.DictReader(csvs[0:], dialect='unix')
            else:
                self.headers = standard_headers
                csvs_to_add = csv.DictReader(csvs, dialect='unix', fieldnames=self.headers)
            for i, d in enumerate(csvs_to_add):
                d[ROWNUM_KEY] = i
                self._add_task(d)
        if len(self.groups) < 1:
            self.groups = [0]
        else:
            self.groups = sorted(self.groups)

    def __exit__(self, exception_type, exception_value, traceback):
        self.update()

    def __enter__(self):
        return self

    def __init__(self, tasksdb_filename="tasks.csv", progress=None, tasksdb_text=None):
        self.tasksdb_text = None
        self.pids = {}
        self.first_group = 0
        self.groups = [0]
        self.cur_group = 0
        self.progress=progress
        self.tasksdb_filename = None
        self.tasksdb_filename_tmp = None
        if tasksdb_filename is not None and tasksdb_text is not None:
            raise Exception('Cannot specify both tasksdb_filename and tasksdb_text.')
        elif tasksdb_filename is not None:
            '''Read in the entire file and keep a copy of it.'''
            tasksdb_filename = str(Path(tasksdb_filename).resolve())
            self.tasksdb_filename = tasksdb_filename
            self.tasksdb_filename_tmp = tasksdb_filename + "~"
            try:
                with open(self.tasksdb_filename, 'r') as tasksdb_f:
                    self.tasksdb_text = tasksdb_f.read()
            except FileNotFoundError:
                self.tasksdb_text = None
        elif tasksdb_text is not None:
            self.tasksdb_text = tasksdb_text
        self.parse()

    def choose_group(self, group):
        self.cur_group = group
        # Needed to refresh the PID list to the new group
        self.tasks()

    def get_num_tasks_by_group(self, group):
        c = 0
        for t in self.rawdata:
            if t['group'] == group:
                c += 1
        return c

    def tasks(self):
        d = []
        self.pids = {}
        for i, t in enumerate(self.rawdata):
            t = dict(t)
            for k, v in t.items():
                if v == '':
                    t[k] = None
            t[ROWNUM_KEY] = i
            if t['group'] is None or t['group'] == self.cur_group or t['group'] < 1:
                if t['pid'] is not None:
                    self.pids[t['pid']] = t
                d.append(t)
        return d

    statuses = {'': 0,
                'IGNORE': 0,
                'NEW': 1,
                'LAUNCHING': 2,
                'RUNNING': 3,
                'FINISHED': 4,
                'ARCHIVING': 5,
                'DELETE': 6,
                'DELETED': 7,
                'INVALID': -1,
                'FAILED': -2,
                'DIED': -3,
                'ZOMBIE': -4,
                'EXCEPTION': -5,
                'KILLING': -6,
                'KILLING9': -7,
                'KILLED': -8,
                'KILLED9': -9,
                'LOST': -10,
                }

    def tasks_by_status(self, status):
        if status.upper() not in self.statuses:
            raise Exception("Status `{}' not recognised.".format(status))
        d = []
        for t in self.tasks():
            if t['status'].upper() == status.upper():
                d.append(t)
        return d

    def task_by_pid(self, pid):
        if pid in self.pids:
            t = self.pids[pid]
            t_pid = t['pid']
            if t_pid != pid:
                raise Exception("Object is not consistent (for {}). t_pid is {} but pid is {}.".format(t['command'], t_pid, pid))
            return self.pids[pid]
        return None

    def set_task(self, t, no_update=False):
        t = dict(t)
        i = t[ROWNUM_KEY]
        old_pid = self.rawdata[i]['pid']
        old_state = self.rawdata[i]['status']
        new_pid = t['pid']
        new_state = t['status']
        if old_pid != new_pid and old_pid != None and new_pid != None:
            raise Exception("A task cannot change its process ID. Attempted from {} to {} for {}.".format(old_pid, new_pid, t['comment']))
        if old_pid != new_pid:
            if old_pid in self.pids:
                del self.pids[old_pid]
            if new_pid is not None:
                self.pids[new_pid] = t
        self.rawdata[i] = t
        if self.progress is not None:
            class StringAsFile:
                def __init__(self):
                    self.s = None

                def write(self, s):
                    self.s = s

            f = StringAsFile()
            w = csv.DictWriter(f, dialect='unix', fieldnames=self.headers)
            nt = dict(t)
            del nt[ROWNUM_KEY]
            if FUNCTION_KEY in nt:
                del nt[FUNCTION_KEY]
            w.writerow(nt)
            self.progress(update_text=f.s, update_fields=t)
        if no_update is not True:
            self.update()

    def set_task_field(self, t, f, d):
        t[f] = d
        self.set_task(t)

    def list_pids(self):
        return list(self.pids.keys())

    def update(self):
        if self.tasksdb_filename_tmp is None or self.tasksdb_filename is None:
            return

        ld = []
        for d in self.rawdata:
            nd = dict(d)
            del nd[ROWNUM_KEY]
            if FUNCTION_KEY in nd:
                del nd[FUNCTION_KEY]
            if 'function' in nd:
                raise Exception('Cannot write a tasks CSV database file when some of the tasks are functions, not external commands.')
            ld.append(nd)

        with open(self.tasksdb_filename_tmp, 'w') as tasksdb_tmp_f:
            tasksdb_tmp_f.write(self.preservetext)
            w = csv.DictWriter(tasksdb_tmp_f, dialect='unix', fieldnames=self.headers)
            w.writeheader()
            w.writerows(ld)

        os.rename(self.tasksdb_filename_tmp, self.tasksdb_filename)

    num_tasks = lambda self: self.rawdata_len

    def add_task(self, **kwds):
        '''add_task has the unique ability to take a lambda for the command argument'''
        t = dict()
        for k in self.headers:
            t[k] = None
        for k, v in kwds.items():
            if k not in self.headers and k not in ['function']:
                raise Exception("Unknown field `{}'".format(k))
            t[k] = v
        self._add_task(t)

    def _add_task(self, t):
        d = dict(t)
        d[ROWNUM_KEY] = self.num_tasks()
        if d['group'] != None and d['group'] != '':
            d['group'] = int(d['group'])
            if d['group'] > 0:
                myset = set(self.groups)
                myset.add(d['group'])
                self.groups = myset
        else:
            d['group'] = None
        if d['pid'] != None and d['pid'] != '':
            pid = int(d['pid'])
            d['pid'] = pid
            if d['group'] is None or d['group'] == '' or d['group'] < 1:
                self.pids[pid] = d
        else:
            d['pid'] = None
        self.rawdata.append(d)
        self.rawdata_len += 1

    def delete_task(self, t):
        '''This fails if the task being deleted is not the very last task.'''
        if t[ROWNUM_KEY] != self.rawdata_len:
            raise Exception('Only the most recently added task may be deleted')
        tt = self.rawdata[-1]
        ttt = dict(t)
        del ttt[ROWNUM_KEY]
        if ttt != tt:
            raise Exception('Task objects are inconsistent')
        del ttt
        del t
        self.rawdata.pop()
        pid = tt['pid']
        group = tt['group']
        del tt
        if pid is not None:
            del self.pids[pid]
        if group is not None and group > 0:
            if self.get_num_tasks_by_group(group) == 0:
                self.groups.remove(group)
                if self.cur_group == group:
                    self.cur_group = 0
                if len(self.groups) < 1:
                    self.first_group = 0
                else:
                    self.first_group = min(self.group)
        self.rawdata_len -= 1

    def delete_all_tasks(self):
        self.pids = {}
        self.first_group = 0
        self.groups = [0]
        self.cur_group = 0
        self.rawdata = []
        self.rawdata_len = 0

def test():
    with QRunnerTasksDatabase() as tdb:
        pass

def main():
    test()

if __name__ == '__main__':
    sys.exit(main())
