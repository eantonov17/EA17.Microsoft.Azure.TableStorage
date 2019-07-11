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
using System;
using System.Collections.Generic;
using System.Text;

namespace EA17.Microsoft.Azure.TableStorage
{

    public class Serializer : StorageSerializer
    {
        internal static readonly DateTime MinDateTime = DateTime.Parse("1601-01-01 0:0:0Z").ToUniversalTime();
        //const string DateTimeFormat = "yyyyMMddHHmmss";
        //const string SerializedTrue = "T";

        static Serializer me;
        public static Serializer Me => me ?? new Serializer();
        public Serializer() => me = me ?? this;

        protected override object PreSerialize(object v)
        {
            if (v is DateTime dt)
                return dt <= MinDateTime ? MinDateTime : dt.ToUniversalTime();
            if (v is DateTimeOffset dto)
                return dto <= MinDateTime ? MinDateTime : dto.UtcDateTime;
            return v;
        }
        protected override object PostDeserialize(object v, Type t)
        {
            if (v is DateTime dt)
                return t.Equals(typeof(DateTime)) ? (dt <= MinDateTime ? (object)DateTime.MinValue : dt)
                    : (dt <= MinDateTime ? (object)DateTimeOffset.MinValue : new DateTimeOffset(dt));
            return v;
        }

        internal static object SerializeValue(object v) => Me.Serialize(v);

        internal static object DeserializeValue(object v, Type t) => Me.Deserialize(v, t);

        //internal static string SerializeKeyValue(Storable key)
        //{
        //    var value = (key ?? throw new ArgumentNullException(nameof(key))).Value;
        //    var typeName = (value ?? throw new InvalidOperationException("Key can't have null Value"))
        //        .GetType().FullName;
        //    switch (typeName)
        //    {
        //        case "System.Guid": return ((Guid)value).ToString("N");
        //        case "System.String": return (string)value;
        //        case "System.Int32":
        //        case "System.Int64": return value.ToString();
        //    }
        //    throw new NotSupportedException("Not supported key type " + typeName);
        //}

        internal static string SerializeKey(Storable key) => SerializeKeyValue(key?.Value ?? throw new InvalidOperationException("Key can't have null Value"));

        internal static string SerializeKeyValue(object value)
        {
            var typeName = (value ?? throw new InvalidOperationException("Key can't have null Value"))
                .GetType().FullName;
            switch (typeName)
            {
                case "System.Guid": return ((Guid)value).ToString("N");
                case "System.String": return (string)value;
                case "System.Int32":
                case "System.Int64": return value.ToString();
            }
            throw new NotSupportedException("Not supported key type " + typeName);
        }

        //internal static void DeserializeKey(Key key, string value)
        //    => key.Stored = DeserializeKeyValue(key.Type, value);

        internal static object DeserializeKeyValue(Type type, string value)
        {
            if (value == null) throw new ArgumentNullException(nameof(value));
            var typeName = (type ?? throw new ArgumentNullException(nameof(type))).FullName;
            switch (typeName)
            {
                case "System.Guid": return Guid.Parse(value);
                case "System.String": return value;
                case "System.Int32": return Int32.Parse(value);
                case "System.Int64": return Int64.Parse(value);
            }
            throw new NotSupportedException("Not supported key type " + typeName);
        }
    }
}
