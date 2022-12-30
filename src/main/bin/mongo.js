/*
 * Copyright (c) 2022. PengYunNetWork
 *
 * This program is free software: you can use, redistribute, and/or modify it
 * under the terms of the GNU Affero General Public License, version 3 or later ("AGPL"),
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 *  without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 *
 *  You should have received a copy of the GNU Affero General Public License along with
 *  this program. If not, see <http://www.gnu.org/licenses/>.
 */

var now = new Date().getTime();
var attr = {
  "dev": NumberLong(0),
  "ino": NumberLong(0),
  "mode": NumberLong(16877),
  "nlink": NumberLong(0),
  "uid": NumberLong(0),
  "gid": NumberLong(0),
  "rdev": NumberLong(0),
  "size": NumberLong(4096),
  "blkSize": NumberLong(512),
  "block": NumberLong(8),
  "atime": NumberLong(now),
  "mtime": NumberLong(now),
  "ctime": NumberLong(now)
};
var node = {
  "_id": "/",
  "_class": "py.fs.Node",
  "attr": attr,
  "dirents": [],
  "pages": [],
  "waitForDelete": false
};
db.node.save(node);
