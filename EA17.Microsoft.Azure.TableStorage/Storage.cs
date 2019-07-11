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

using EA17.ClassLibrary.Fundamentals;
using EA17.ClassLibrary.Storage;
using Microsoft.Azure.Cosmos.Table;
using System;

namespace EA17.Microsoft.Azure.TableStorage
{
    public class Storage : StorageBase
    {
        public static Storage Get(string storageName = null) => (Storage)Get(typeof(Storage), storageName ?? "TableStorage", New);
        private static IStorage New(string storageName) => new Storage(storageName);

        public static Storage DefaultStorage { get; } = Get();

        private readonly CloudTableClient client;
        private Storage(string storageName) : base(storageName)
        {
            // .Net Framework way
            //var connectionString = CloudConfigurationManager.GetSetting(Name + "ConnectionString");

            var connectionString = CloudConfigurationManager.GetSetting(Name + "ConnectionString");
            var account = CloudStorageAccount.Parse(connectionString);
            client = account.CreateCloudTableClient();
        }

        protected override IStorageAdapter CreateAdapter(Type t) => new Adapter(client, t);

        public static readonly Tag PartitionKey = new Tag("PartitionKey");
        public static readonly Tag RowKey = new Tag("RowKey");
    }
}
