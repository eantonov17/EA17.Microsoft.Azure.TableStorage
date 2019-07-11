//  # EA17.Microsoft.Azure.TableStorage
//
//  Copyright (c) 2018-2019 Eugene Antonov
//
//  This program is free software: you can redistribute it and/or modify
//  it under the terms of version 3 of the GNU General Public License
//  as published by the Free Software Foundation.
//
//  This program is distributed in the hope that it will be useful,
//  but WITHOUT ANY WARRANTY; without even the implied warranty of
//  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.See the
//  GNU General Public License for more details.
//
//  You should have received a copy of the GNU General Public License
//  along with this program.If not, see<https://www.gnu.org/licenses/>.

using EA17.ClassLibrary.Storage;
using Microsoft.Azure.Cosmos.Table;
using System;
using System.Collections.Generic;

namespace EA17.Microsoft.Azure.TableStorage
{
    public class Adapter : IStorageAdapter
    {
        private readonly CloudTable cloudTable;
        private readonly Type t;

        internal Adapter(CloudTableClient client, Type t)
        {
            cloudTable = client.GetTableReference(t.Name);
            cloudTable.CreateIfNotExists();
            this.t = t;
        }

        public IStorageEntity GetEntity(StorableObject so) => new Entity(cloudTable, so);

        public IEnumerable<IStorageEntity> Select(string query, int? maxCount, IList<string> columns)
        {
            var dtes = cloudTable.ExecuteQuery(new TableQuery()
            { FilterString = query, TakeCount = maxCount, SelectColumns = columns });
            foreach (var dte in dtes)
                yield return new Entity(cloudTable, t, dte);
        }

        static string QFormat(object v)
        {
            Type type = v.GetType();
            string format = null;
            switch (type.FullName)
            {
                case "System.String":
                case "System.DateTime":
                case "System.DateTimeOffset":
                    format = "'{0}'"; break;
                case "System.Guid":
                    format = "guid'{0}'"; break;
                case "System.Int32":
                case "System.Boolean":
                //case "System.Byte[]":???
                case "System.Int64":
                case "System.Double":
                    format = "{0}"; break;
                default:
                    throw new NotSupportedException($"Type {type?.FullName} is not supported as key in table queries");
            }
            return string.Format(format, v);
        }

        public IEnumerable<IStorageEntity> Select(StorableObject so, int? maxCount)
        {
            var pkName = so.Untag(PartitionKey);
            var rkName = so.Untag(RowKey);

            var qb = new QueryBuilder();
            foreach ((StorableProperty property, string op, object v) in so.EnumConditions())
            {
                if (property.Name == pkName)
                    qb.Add(op, PartitionKey.String, QFormat(Serializer.SerializeKeyValue(v)));
                else if (property.Name == rkName)
                    qb.Add(op, RowKey.String, QFormat(Serializer.SerializeKeyValue(v)));
                else
                    qb.Add(op, property.Name, QFormat(Serializer.SerializeValue(v)));
            }
            return Select(qb.ToString(), maxCount, so.FieldSelection.List);
        }
    }

}
