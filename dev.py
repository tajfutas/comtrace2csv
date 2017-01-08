import collections
import datetime
import re

fpath = r'..\data-meteor2016\sicomtrace\day1\finish.log'
fpath = r'..\data-meteor2016\sicomtrace\day1\readout.log'

restr = r'^(\d{4}/[01]\d/[0-3]\d [0-2]\d:[0-5]\d:[0-5]\d\.\d+) ([^\r\n]+?)(<?)-\(trace\)-(>?)([^\r\n]*?): MSG_LINE_DATA\s{\[(\d+)\]:(.+?)}$'

reobj = re.compile(restr,  re.MULTILINE | re.DOTALL)

with open(fpath) as f:
    trace = f.read()

# http://stackoverflow.com/a/312464/2334951
def chunks(seq, n):
    """Yield successive n-sized chunks from seq."""
    for i in range(0, len(seq), n):
        yield seq[i:i + n]


def traffic_iter(trace):
    for m in reobj.finditer(trace):
        groups = m.groups()
        dt = datetime.datetime.strptime(groups[0] + '000', '%Y/%m/%d %H:%M:%S.%f')
        if groups[2] and not groups[3]:
            nodes = groups[4], groups[1]
        elif groups[3] and not groups[2]:
            nodes = groups[1], groups[4]
        else:
            s = m.string[slice(*m.span())]
            raise TypeError('Invalid format: {}'.format(s[:48]))
        dlen = int(groups[5])
        dtext = ' '.join(s[3:50].rstrip() for s in chunks(groups[6], 72))
        data = bytes(int(s[:2],16) for s in chunks(dtext, 3))
        assert dlen == len(data)
        yield (dt,) + nodes + (data,)


it = traffic_iter(trace)

tr = list(it)
