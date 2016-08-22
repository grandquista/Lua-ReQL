#!/usr/bin/env python
"""parsePolyglot."""

import os
import re
import sys

# == globals

try:
    unicode
except NameError:
    unicode = str

printDebug = False

commentLineRegex = re.compile(r'^\s*#')
yamlLineRegex = re.compile(r'''
    ^(?P<indent>\ *)((?P<itemMarker>-\ +)(?P<itemContent>.*)$|
    ^((?P<key>[\w\.]+)(?P<keyExtra>:\ *))?(?P<content>.*))\s*$
    ''', re.VERBOSE)

# ==


class yamlValue(unicode):
    """yamlValue."""

    linenumber = None

    def __new__(cls, value, linenumber=None):
        """__new__."""
        if isinstance(value, unicode):
            real = unicode.__new__(cls, value)
        else:
            real = unicode.__new__(cls, value, "utf-8")
        if linenumber is not None:
            real.linenumber = int(linenumber)
        return real

    def __repr__(self):
        """__repr__."""
        real = super(yamlValue, self).__repr__()
        return real.lstrip('u')


def debug(message):
    """debug."""
    if printDebug and message:
        message = str(message).rstrip()
        if message:
            print(message)
            sys.stdout.flush()


def expectYAML(returnItem, linenumber, line, expected):
    """expectYAML."""
    if not isinstance(returnItem, expected):
        raise Exception(
            (
                'Bad YAML, got a {} item while working on a {} '
                'on line {}: {}'
            ).format(
                expected.__class__.__name__,
                returnItem.__class__.__name__,
                linenumber,
                line.rstrip()))


def parseYAML_inner(source, indent):
    """parseYAML_inner."""
    returnItem = None

    for linenumber, line in source:
        debug('line %d (%d):%s' % (linenumber, indent, line))

        # empty or comment line, ignore
        if line.strip() == '' or commentLineRegex.match(line):
            debug('\tempty/comment line')
            continue

        # - parse line

        parsedLine = yamlLineRegex.match(line)
        if not parsedLine:
            raise Exception('Unparseable YAML line {}: {}'.format(
                linenumber, line.rstrip()))

        lineIndent = len(parsedLine.group('indent'))
        lineItemMarker = parsedLine.group('itemMarker')
        lineKey = parsedLine.group('key') or ''
        lineKeyExtra = parsedLine.group('keyExtra') or ''
        lineContent = (
            parsedLine.group('content') or
            parsedLine.group('itemContent') or
            '').strip()

        # - handle end-of-sections
        if lineIndent < indent:
            # we have dropped out of this item, push back the line and
            # return what we have
            source.send((linenumber, line))
            debug('\tout one level')
            return returnItem

        # - array item
        if lineItemMarker:
            debug('\tarray item')
            # item in an array
            if returnItem is None:
                debug('\tnew array, indent is %d' % lineIndent)
                returnItem = []
                indent = lineIndent
            else:
                expectYAML(returnItem, linenumber, line, list)
            indentLevel = lineIndent + len(lineItemMarker)
            source.send((linenumber, (' ' * (indentLevel)) + lineContent))
            returnItem.append(
                parseYAML_inner(source=source, indent=indent + 1))
            continue

        # - dict item
        if lineKey:
            debug('\tdict item')
            if returnItem is None:
                debug('\tnew dict, indent is %d' % lineIndent)
                # new dict
                returnItem = {}
                indent = lineIndent
            else:
                expectYAML(returnItem, linenumber, line, dict)
            indentLevel = lineIndent + len(lineKey) + len(lineKeyExtra)
            source.send((linenumber, (' ' * indentLevel) + lineContent))
            returnItem[lineKey] = parseYAML_inner(
                source=source,
                indent=indent + 1)
            continue

        # - data - one or more lines of text
        debug('\tvalue')
        if returnItem is None:
            returnItem = yamlValue('', linenumber)
            if lineContent.strip() in ('|', '|-', '>'):
                continue  # yaml multiline marker
        else:
            expectYAML(returnItem, linenumber, line, yamlValue)
        if returnItem:
            returnItem = yamlValue(
                returnItem + "\n" + lineContent,
                returnItem.linenumber)  # str subclasses are not fun
        else:
            returnItem = yamlValue(lineContent, linenumber)
    return returnItem


def parseYAML_generator(source):
    """parseYAML_generator."""
    if hasattr(source, 'capitalize'):
        if os.path.isfile(source):
            source = open(source, 'r')
        else:
            source = source.splitlines(True)
    elif hasattr(source, 'readlines'):
        pass  # the for loop will already work

    backlines = []
    for linenumber, line in enumerate(source):
        backline = None
        usedLine = False
        while usedLine is False or backlines:
            if backlines:
                backline = yield backlines.pop()
            else:
                usedLine = True
                backline = yield (linenumber + 1, line)
            while backline:  # loops returning None for every send()
                assert isinstance(backline, tuple)
                assert isinstance(backline[0], int)
                backlines.append(backline)
                backline = yield None


def parseYAML(source):
    """parseYAML."""
    return parseYAML_inner(parseYAML_generator(source), indent=0)

if __name__ == '__main__':
    import optparse
    import pprint

    parser = optparse.OptionParser()
    parser.add_option(
        "-d",
        "--debug",
        dest="debug",
        action="store_true",
        default=False,
        help="print debug information")
    (options, args) = parser.parse_args()
    printDebug = options.debug

    if len(args) < 1:
        parser.error(
            '{} needs files to process'.format(os.path.basename(__file__)))

    for filePath in args:
        if not os.path.isfile(filePath):
            sys.exit('target is not an existing file: {}'.format(
                os.path.basename(__file__)))

    for filePath in args:
        print('=== %s' % filePath)
        pprint.pprint(parseYAML(filePath))
