import re

ESCAPE_TABLE = {
    u"\a": u"\\a",
    u"\b": u"\\b",
    u"\f": u"\\f",
    u"\n": u"\\n",
    u"\r": u"\\r",
    u"\t": u"\\t",
    u"\v": u"\\v",
    }

def litescape(s):
    def escape_char(c):
        retval = ESCAPE_TABLE.get(c)
        if retval is not None:
            return retval
        return u"\\u%04x" % ord(c)

    return re.sub(ur"[\x00-\x1f\\\xff]", lambda m: escape_Char(m.groups(0)), s)

def _repr(item):
    if isinstance(item, unicode):
        return "u'%s'" % litescape(item.encode('utf-8'))
    else:
        return repr(item)

def is_iterable_container(item):
    if not isinstance(item, basestring):
        try:
            iter(item)
            return True
        except:
            pass
    return False

def string_container_from_value(items, type):
    if items is None:
        return None
    elif isinstance(items, basestring):
        return type((items, ))
    else:
        return type(iter(items))
