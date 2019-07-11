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

namespace EA17.Microsoft.Azure.TableStorage
{
    public class Entity : IStorageEntity
    {
        private readonly CloudTable cloudTable;
        public StorableObject Value { get; }
        private string ETag { get; set; }
        public bool IsRetrieved => ETag != null;

        public static bool StoreShadowValues = false;

        internal Entity(CloudTable cloudTable, StorableObject so)
        {
            this.cloudTable = cloudTable;
            Value = so;
        }

        internal Entity(CloudTable cloudTable, Type t, DynamicTableEntity dte) : this(cloudTable, StorableProperties.Get(t).NewInited()) => ETag = DteToEntity(Value, dte);

        private static string DteToEntity(StorableObject storableObject, TableResult result)
            => DteToEntity(storableObject, result?.Result as DynamicTableEntity);

        //internal static void DteToKey(Storable key, string keyName, string serializedKeyValue)
        //{
        //    var keyValue = Serializer.DeserializeKeyValue(key.Type, serializedKeyValue);
        //    if (key == null)
        //        throw new InvalidOperationException(keyName + " must be not null");
        //    if (key.Value == null)
        //        key.Value = keyValue;
        //    else if (!key.Value.Equals(keyValue))
        //        throw new InvalidOperationException(keyName + " mismatch");
        //}

        private static string DteToEntity(StorableObject so, DynamicTableEntity dte)
        {
            if (dte == null) return null;

            var s = so[Storage.PartitionKey];
            s.Stored = Serializer.DeserializeKeyValue(s.Type, dte.PartitionKey);

            s = so[Storage.RowKey];
            s.Stored = Serializer.DeserializeKeyValue(s.Type, dte.RowKey);

            foreach (var kv in dte.Properties)
            {
                s = so[kv.Key];
                s.Stored = Serializer.DeserializeValue(kv.Value.PropertyAsObject, s.Type);
            }
            return dte.ETag;
        }

        private static DynamicTableEntity DteKeysFromEntity(StorableObject so, string eTag) => new DynamicTableEntity()
        {
            ETag = eTag ?? "*",
            PartitionKey = Serializer.SerializeKeyValue(so[Storage.PartitionKey].Value),
            RowKey = Serializer.SerializeKeyValue(so[Storage.RowKey].Value)
        };

        private static DynamicTableEntity DteFromEntity(StorableObject so, string eTag)
        {
            var dte = DteKeysFromEntity(so, eTag);

            var pkName = so.Untag(Storage.PartitionKey);
            var rkName = so.Untag(Storage.RowKey);

            foreach ((StorableProperty property, object value) in so.EnumStorables(allProps: eTag == null))
            {
                if (value == null || property.Name == pkName || property.Name == rkName)
                    continue;
                var name = property.Name;
                var serialized = Serializer.SerializeValue(value);
                dte.Properties[name] = EntityProperty.CreateEntityPropertyFromObject(serialized);
                if (StoreShadowValues && Serializer.Me.IsSerializedValue(value))
                    dte.Properties[name + (char)0x34C] = EntityProperty.CreateEntityPropertyFromObject(value.ToString());
            }
            return dte;
        }

        public IStorageEntity Retrieve()
        {
            var result = cloudTable.Execute(TableOperation.Retrieve<DynamicTableEntity>(Serializer.SerializeKey(Value[Storage.PartitionKey]), Serializer.SerializeKey(Value[Storage.RowKey])));
            ETag = DteToEntity(Value, result);
            return this;
        }
        public IStorageEntity InsertOrMerge()
        {
            var dte = DteFromEntity(Value, ETag);
            if (dte.Properties.Count > 0)
            {
                var result = IsRetrieved ?
                    cloudTable.Execute(TableOperation.Merge(dte)) :
                    cloudTable.Execute(TableOperation.Insert(dte));
                ETag = DteToEntity(Value, result);
            }
            return this;
        }
        public IStorageEntity InsertOrReplace()
        {
            var dte = DteFromEntity(Value, null);
            if (dte.Properties.Count > 0)
            {
                var result = cloudTable.Execute(TableOperation.InsertOrReplace(dte));
                ETag = DteToEntity(Value, result);
            }
            return this;
        }
        public bool Delete()
        {
            var dte = DteKeysFromEntity(Value, ETag);
            var result = cloudTable.Execute(TableOperation.Delete(dte));
            ETag = null;
            return result.HttpStatusCode == 200;
        }

        public override string ToString() => Value.ToString() + " , ETag=" + ETag;
    }
}
