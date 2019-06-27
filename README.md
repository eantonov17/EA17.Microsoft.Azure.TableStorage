# EA17.Microsoft.Azure.TableStorage

**C# .Net Core library to simplify work with Microsoft Azure TableStorage**

*Copyright (C) 2018-2019 Eugene Antonov*

This program is free software: you can redistribute it and/or modify
it under the terms of version 3 of the GNU General Public License 
as published by the Free Software Foundation .

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program.  If not, see <https://www.gnu.org/licenses/>.

## Some of issues solved by this library:
1. track of values loaded from a table. if a value has been changed then an update will include it, if not the value is excluded - 
this reduces traffic and cost
1. values are tracked on the object level, so you don't have to send a whole large objecty just to update an int
1. automatic conversion to/from table data types

I'll add more as i'm progressing through refactoring of an older code and adding it here.