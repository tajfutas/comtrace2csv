import collections
import datetime
import itertools
import pathlib
import re

entry = collections.namedtuple('Entry',
    ('time', 'source', 'dest', 'bytes'))
ENTRY_RESTR = (r'^(\d{4}/[01]\d/[0-3]\d '
    r'[0-2]\d:[0-5]\d:[0-5]\d\.\d+) '
    r'([^\r\n]+?)(<?)-\(trace\)-(>?)([^\r\n]*?): '
    r'MSG_LINE_DATA\s{\[(\d+)\]:(.*?)}$')
entry_reobj = re.compile(ENTRY_RESTR,  re.MULTILINE | re.DOTALL)


# http://stackoverflow.com/a/312464/2334951
def chunks(seq, n):
    """Yield successive n-sized chunks from seq."""
    for i in range(0, len(seq), n):
        yield seq[i:i + n]


def entry_iter(tracestr):
  for m in entry_reobj.finditer(tracestr):
    groups = m.groups()
    dt = datetime.datetime.strptime(groups[0] + '000',
        '%Y/%m/%d %H:%M:%S.%f')
    if groups[2] and not groups[3]:
      nodes = groups[4], groups[1]
    elif groups[3] and not groups[2]:
      nodes = groups[1], groups[4]
    else:
      s = m.string[slice(*m.span())]
      raise TypeError('Invalid format: {}'.format(s[:48]))
    dlen = int(groups[5])
    dtext = ' '.join(s[3:50].rstrip()
        for s in chunks(groups[6], 72))
    data = bytes(int(s[:2],16) for s in chunks(dtext, 3))
    assert dlen == len(data)
    yield entry(dt, *nodes, data)


def glob_entry_iter(globstr):
  p_iter = pathlib.Path().glob(globstr)
  for p in sorted(p_iter):
    with p.open() as f:
      yield from entry_iter(f.read())


def traffic_iter(entry_iter_inst, time_diff_tolerance=None):
  if time_diff_tolerance is None:
    time_diff_tolerance = datetime.timedelta(0)
  elif not isinstance(time_diff_tolerance, datetime.timedelta):
    time_diff_tolerance = datetime.timedelta(0,
        time_diff_tolerance)
  ref_entry = next(entry_iter_inst)
  ref_data = ref_entry.bytes
  for e in entry_iter_inst:
    if e[1:3] == ref_entry[1:3]:
      if e.time - ref_entry.time <= time_diff_tolerance:
        ref_data += e.bytes
        continue
    yield entry(ref_entry.time, ref_entry.source,
        ref_entry.dest, ref_data)
    ref_entry = e
    ref_data = ref_entry.bytes
  else:
    yield entry(ref_entry.time, ref_entry.source,
        ref_entry.dest, ref_data)


if __name__ == '__main__':
  globstr = r'..\data-meteor2016\sicomtrace\day1\finish.log'
  #globstr = r'..\data-oevb2016\sicomtrace\finish*.log'
  geit = glob_entry_iter(globstr)
  trit = traffic_iter(geit, 1)
  tr = list(trit)

  for x in tr:
    print(x[:-1], len(x.bytes))
