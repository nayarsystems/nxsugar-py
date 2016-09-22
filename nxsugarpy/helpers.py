# -*- coding: utf-8 -*-
##############################################################################
#
#    nxsugarpy, a Python library for building nexus services with python
#    Copyright (C) 2016 by the nxsugarpy team
#
#    This program is free software: you can redistribute it and/or modify
#    it under the terms of the GNU Lesser General Public License as published
#    by the Free Software Foundation, either version 3 of the License, or
#    (at your option) any later version.
#
#    This program is distributed in the hope that it will be useful,
#    but WITHOUT ANY WARRANTY; without even the implied warranty of
#    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#    GNU Lesser General Public License for more details.
#
#    You should have received a copy of the GNU Lesser General Public License
#    along with this program.  If not, see <http://www.gnu.org/licenses/>.
#
##############################################################################

def secondsToStr(secs):
    out = ""
    if secs < 1:
        return "{0:.4f}s".format(secs)
    elif secs < 60:
        return "{0:.0f}s".format(secs)
    elif secs < 3600:
        mins = secs/60
        rsecs = secs%60
        out = "{0:.0f}m".format(mins)
        if rsecs > 0:
            out = "{0}{1:.0f}s".format(out, rsecs)
    else:
        hours = secs / 3600
        rsecs = secs % 3600
        out = "{0:.0f}h".format(hours)
        if rsecs > 0:
            rmins = rsecs / 60
            rsecs = rsecs % 60
            out = "{0}{1:.0f}m".format(out, rmins)
            if rsecs > 0:
                out = "{0}{1:.0f}s".format(out, rsecs)
    return out