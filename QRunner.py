#!/usr/bin/env python3

import csv, re, sys, os, getpass, platform, subprocess, psutil, shlex

import QRunnerTasksDatabase

class QRunner:

    def __exit__(self, exception_type, exception_value, traceback):
        self.tdb.update()

    def __enter__(self):
        return self

    def __init__(self, timeout=10, killtimeout=2, progress=None, max_tasks=64, **kwds):
        self.timeout = timeout
        self.killtimeout = killtimeout
        self.tdb = QRunnerTasksDatabase.QRunnerTasksDatabase(progress=progress, **kwds)
        self.popens = {}
        self.max_tasks = max_tasks
        self.progress = progress
        self.original_cwd = None
        pass

    def _launch_task(self, t, comment='', status='INVALID', rownum=None, 
                             pid=None, rc=None, command=None, group=0, user=getpass.getuser(),
                             host=platform.node(), pwd=None, inputfile=None, outputfile=None,
                             errorfile=None):

        if command == None:
            raise Exception("Cannot have a task with no command.")

        if host != platform.node():
            raise Exception("Executing tasks on a remote host is not yet supported. \
The local host is `{}'.".format(platform.node()))

        if user != getpass.getuser():
            raise Exception("Executing tasks as another user is not yet supported. \
The current user is `{}'.".format(getpass.getuser()))

        group = int(group)

        if pwd != None:
            if self.original_cwd is None:
                self.original_cwd = os.getcwd()
            else:
                os.chdir(self.original_cwd)
            pwd = os.path.expanduser(pwd)
            try:
                os.chdir(pwd)
            except FileNotFoundError:
                try:
                    os.makedirs(pwd)
                except OSError as e:
                    if e.errno != errno.EEXIST:
                        raise
                os.chdir(pwd)

        if inputfile == '-':
            inputf=sys.stdin
        if outputfile == '-':
            outputf=sys.stdout
        if errorfile == '-':
            errorf=sys.stderr

        if inputfile is None:
            inputfile = '{}-{}-{}.in.txt'.format(group, t['rownum'], comment)
        try:
            inputf = open(inputfile, "r")
        except FileNotFoundError:
            inputf = subprocess.DEVNULL

        if outputfile is None:
            outputfile = '{}-{}-{}.out.txt'.format(group, comment, t['rownum'])
        outputf = open(outputfile, "w")

        if errorfile is None:
            errorfile = '{}-{}-{}.err.txt'.format(group, comment, t['rownum'])
        errorf = open(errorfile, "w")

        t['status'] = 'LAUNCHING'
        self.tdb.set_task(t, no_update=True)
        p = subprocess.Popen(shlex.split(command), stdin=inputf, stdout=outputf, stderr=errorf)
        t['pid'] = p.pid
        t['status'] = 'RUNNING'
        self.popens[p.pid] = p
        self.tdb.set_task(t, no_update=True)
        self.done_tasks += 0.5

        if self.original_cwd is not None:
            os.chdir(self.original_cwd)

    def launch_task(self, t):
        tt = {}
        for k, v in t.items():
            if v is not None and k in self.tdb.headers and k != 'exception':
                tt[k] = t[k]
        self._launch_task(t, **tt)

    def done(self):
        for t in self.tdb.tasks():
            if t['status'] in ['NEW', 'LAUNCHING', 'RUNNING', 'KILLING', 'KILLING9']:
                return False
        return True

    def check(self):
        all_pids = psutil.pids()
        # Check for tasks we think are still running but aren't
        popens_c = self.popens.keys()
        for pid in self.tdb.list_pids():
            if pid not in all_pids:
                if pid not in popens_c:
                    self.died(self.tdb.task_by_pid(pid))
                else:
                    # Check for tasks Popen got before our own os.wait().
                    self.finished(pid, self.popens[pid].returncode)

        for t in self.tdb.tasks_by_status('LAUNCHING'):
            t['status'] = 'FAILED'
            t['pid'] = None
            t['rc'] = None
            self.tdb.set_task(t)
        for t in self.tdb.tasks_by_status('KILLING9'):
            pid = t['pid']
            if pid in all_pids:
                self.tdb.set_task_field(t, 'status', 'ZOMBIE')
            else:
                t = dict(t)
                t['status'] = 'KILLED9'
                t['pid'] = None
                t['rc'] = -9
                self.tdb.set_task(t)
        for t in self.tdb.tasks_by_status('LOST'):
            raise Exception("I don't expect this to happen")
            self.died(t)

    def died(self, t):
        pid = t['pid']
        if t['status'] != 'RUNNING':
            return
        if pid in psutil.pids():
            t['status'] = 'LOST'
        else:
            t = dict(t)
            t['pid'] = None
            t['status'] = 'DIED'
        self.tdb.set_task(t)

    def finished(self, pid, rc):
        t = self.tdb.task_by_pid(pid)
        t = dict(t)
        t['status'] = 'FINISHED'
        t['pid'] = None
        t['rc'] = str(rc)
        self.tdb.set_task(t, no_update=True)
        del self.popens[pid]
        self.done_tasks += 0.5

    def wait(self):
        pids_to_wait = {}
        for pid in self.tdb.list_pids():
            pids_to_wait[pid] = True
        more_tasks = True
        while len(pids_to_wait) > 0:
            try:
                pid, rc = os.wait()
            except ChildProcessError:
                # Popen managed to call wait before we did
                for pid2 in pids_to_wait:
                    if pid2 in self.popens:
                        self.finished(pid, self.popens[pid].returncode)
                    else:
                        raise Exception("Process ID {} inexplicably never returned.".format(pid))
            self.call_progress()
            if pid not in pids_to_wait:
                raise Exception("Did not have process ID {} in my list".format(pid))
            self.finished(pid, rc)
#            del pids_to_wait[pid]
            if more_tasks is True:
                more_tasks = self.launch()
            pids_to_wait = {}
            for pid in self.tdb.list_pids():
                pids_to_wait[pid] = True

    def launch(self):
        self.check()
        cur_tasks = len(self.tdb.list_pids())
        l = self.tdb.tasks_by_status('NEW')
        if len(l) < 1:
            return False
        for i, t in enumerate(l):
            if (i + cur_tasks) > self.max_tasks:
                return True
#            try:
#                self.launch_task(t)
            self.launch_task(t)
#            except Exception as e:
#                t['status'] = 'EXCEPTION'
#                t['exception'] = str(e)
#                self.tdb.set_task(t)
            self.call_progress()
        return True

    def call_progress(self):
        if self.progress is not None:
            self.progress(percentage=self.calculate_percentage())

    def calculate_percentage(self):
        grp_amt = 1 / self.num_groups
        grps_done = self.done_groups / self.num_groups
        if self.num_tasks > 0:
            tasks_done = self.done_tasks / self.num_tasks
        else:
            tasks_done = 0
        tasks_done *= grp_amt
        total_done = grps_done + tasks_done
        return str(round(100 * total_done))

    def run(self):
        groups = self.tdb.groups
        self.num_groups = len(groups)
        self.done_groups = 0
        for g in groups:
            self.done_tasks = 0
            self.tdb.choose_group(g)
            self.num_tasks = len(self.tdb.tasks_by_status('NEW'))
            self.check()
            self.launch()
            self.wait()
            self.check()
            self.done_groups += 1
        self.done_tasks = 0
        self.call_progress()
        self.tdb.update()

def test():
    def print_dots(update_text=None, update_fields=None, percentage=None):
        shown_progress = True
        if percentage != None:
            print("\r{}%   ".format(percentage), end='')
        sys.stdout.flush()
    print("         Processing queue ...", end='')
    sys.stdout.flush()
    with QRunner(progress=print_dots, max_tasks=256) as qr:
        qr.run()
        if qr.done() is False:
            print()
            raise Exception("qr.done() returned False")
        print(" done.                     ")

def main():
    test()

if __name__ == '__main__':
    sys.exit(main())
