from lxml import etree
import requests
import os
from PIL import Image
from io import BytesIO
import yaml
from tqdm import tqdm
import collections
import xmltodict
from bs4 import BeautifulSoup
import json
import urllib.request


class Set:
    def __init__(self, search_string, yaml_settings):
        """Initializes an instance of a Set with a search string and contents of a yaml config.

        Args:
            search_string (str): A fedora search string to find PIDS related to a query.
            yaml_settings (dict): A dict of various setting predefined by the user in a config file.

        """
        self.size = 0
        self.results = []
        self.request = search_string
        self.settings = yaml_settings
        self.token = ""

    def __repr__(self):
        return f"A set of records based on the following http request:\n\t{self.request}."

    def __str__(self):
        return f"A set of records based on the following http request:\n\t{self.request}."

    def populate(self):
        """Populates the results property of the Sets instance.

        Populates the results property of the Sets instance with every pid that is associated with a request. The
        results property is intended to be used by all other methods to determine which pids the method should be run
        against.

        Returns:
            None

        Examples:
            >>>Set('http://localhost:8080', yaml.safe_load(open("config.yml", "r"))).populate()
            None

        """
        document = etree.parse(f"{self.request}{self.token}")
        token = document.xpath('//types:token', namespaces={"types": "http://www.fedora.info/definitions/1/0/types/"})
        results = document.findall('//{http://www.fedora.info/definitions/1/0/types/}pid')
        print(".", end="", flush=True)
        for result in results:
            self.results.append(result.text)
            self.size += 1
        if len(token) == 1:
            self.token = f"&sessionToken={token[0].text}"
        else:
            self.token = None
        return

    def count_objects(self):
        """Returns number of pids that match query.

        Return the length of the list of pids that match a query.

        Returns:
             int: The number of pids that match a query.

        Examples:
            >>>Set('http://localhost:8080', yaml.safe_load(open("config.yml", "r"))).populate()
            62000

        """
        return len(self.results)

    def harvest_metadata(self, dsid="MODS"):
        """Harvests metadata and other text or xml datastreams.

        Accepts a datastream id (MODS by default) and serializes the datastream to disk according to the contents of
        self.results

        Args:
            dsid (str): The datastream id of the objects you want to download and serialize to disk. MODS by default.

        Returns:
            dict: A dict with the number of files in attempted to download, the dsid that was used, and a list of errors
            as tuples with the PID and the status code of the request.

        Examples:
            >>> Set('http://localhost:8080', yaml.safe_load(open("config.yml", "r"))).harvest_metadata("MODS")
            {'Attempted Downloads': 3, 'dsid': 'MODS', 'errors': []}
            >>> Set('http://localhost:8080', yaml.safe_load(open("config.yml", "r"))).harvest_metadata("DARWIN")
            {'Attempted Downloads': 3, 'dsid': 'DARWIN', 'errors': [('test:4', 404), ('test:5', 404), ('test:6', 404)]}

        """
        if self.settings["destination_directory"] in os.listdir("."):
            pass
        else:
            os.mkdir(self.settings["destination_directory"])
        errors = []
        for result in tqdm(self.results):
            r = requests.get(f"{self.settings['fedora_path']}:{self.settings['port']}/fedora/objects/{result}/"
                             f"datastreams/{dsid}/content",
                             auth=(self.settings['username'], self.settings['password']))
            r.encoding = "utf-8"
            if r.status_code == 200:
                new_name = result.replace(":", "_")
                ext = r.headers["Content-Type"].split(";")[0].split("/")[1]
                with open(f"{self.settings['destination_directory']}/{new_name}.{ext}", "w") as new_file:
                    new_file.write(r.text)
            else:
                errors.append((result, r.status_code))
                print(f"Could not harvest metadata for {result}: {r.status_code}.")
        print(f"\n\nDownloaded {len(self.results)} {dsid} records.")
        return {"Attempted Downloads": len(self.results), "dsid": dsid, "errors": errors}

    def find_content_types(self):
        """Returns all content models found in a request.

        Returns a list of all content types found in a current request.

        Returns:
            list: A list of all content types found in a request.

        Examples:
            >>> Set('http://localhost:8080', yaml.safe_load(open("config.yml", "r"))).find_content_types()
            ['sp_basic_image', 'compoundCModel']

        """
        content_types = []
        for result in tqdm(self.results):
            x = Record(result).find_content_type()
            if x not in content_types:
                content_types.append(x)
        return content_types

    def grab_images(self, dsid="TN"):
        """Attempts to serialize an image to disk. (Deprecated)

        This is an old method that was used for downloading images.  I highly recommend using grab_binary() instead.

        Args:
            dsid (str): The image datastream id to download.  Defaults to TN.

        Returns:
            dict: A dict with the number of attempted downloads, the datastream id that was passed, and a list of errors
            as tuples with the PID and the http status code.

        Examples:
            >>> Set('http://localhost:8080', yaml.safe_load(open("config.yml", "r"))).grab_images("TN")
            {'Attempted Downloads': 3, 'dsid': 'TN', 'errors': []}

        """
        if self.settings["destination_directory"] in os.listdir("."):
            pass
        else:
            os.mkdir(self.settings["destination_directory"])
        if dsid is None:
            dsid = self.settings["default_dsid"]
        errors = []
        for result in tqdm(self.results):
            r = requests.get(f"{self.settings['fedora_path']}:{self.settings['port']}/fedora/objects/{result}/"
                             f"datastreams/{dsid}/content",
                             auth=(self.settings['username'], self.settings['password']))
            if r.status_code == 200:
                ext = r.headers["Content-Type"].split(";")[0].split("/")[1]
                in_file = Image.open(BytesIO(r.content))
                new_name = result.replace(":", "_")
                in_file.save(f"{self.settings['destination_directory']}/{new_name}.{ext}")
            else:
                errors.append((result, r.status_code))
        return {"Attempted Downloads": len(self.results), "dsid": dsid, "errors": errors}

    def grab_binary(self, dsid="OBJ"):
        """ Serializes binaries to disk for a specific search.

        Accepts a datastream ID (OBJ by default) and serializes it to disk for a specific search.

        Args:
            dsid (str): The id of the datastream you want to serialize to disk.

        Returns:
            dict: A dictionary with the PIDs of attempted downloads, the datastream id, and a list of errors as tuples
            with the PID and http status code.

        Examples:
            >>> Set('http://localhost:8080', yaml.safe_load(open("config.yml", "r"))).grab_binary("TN")
            {'Attempted Downloads': ['test:4', 'test:5', 'test:6'], 'dsid': 'TN', 'errors': []}
            >>> Set('http://localhost:8080', yaml.safe_load(open("config.yml", "r"))).grab_binary("OBJ")
            {'Attempted Downloads': ['test:4', 'test:5', 'test:6'], 'dsid': 'OBJ', 'errors': [('test:5', 404)]}

        """
        if self.settings["destination_directory"] in os.listdir("."):
            pass
        else:
            os.mkdir(self.settings["destination_directory"])
        errors = []
        for result in tqdm(self.results):
            r = requests.get(f"{self.settings['fedora_path']}:{self.settings['port']}/fedora/objects/{result}/"
                             f"datastreams/{dsid}/content",
                             auth=(self.settings['username'], self.settings['password']))
            if r.status_code == 200:
                new_name = result.replace(":", "_")
                ext = r.headers["Content-Type"].split(";")[0].split("/")[1]
                with open(f"{self.settings['destination_directory']}/{new_name}.{ext}", 'wb') as other:
                    other.write(r.content)
            else:
                errors.append((result, r.status_code))
        return {"Attempted Downloads": self.results, "dsid": dsid, "errors": errors}

    def write_datastream_history(self, dsid, result_format="xml"):
        """Serializes the datastream history of a specific dsid for all results in a query.

        Accepts a datastream id and a result format (xml by default) and serializes the history to indvidual files on
        disk.

        Args:
            dsid (str): The datastream id to retrieve history on.
            result_format (str): The result format to serialize to disk as. xml (default) or html

        Returns:
            dict: A dict with the number of attempted history files to serialize, the dsid for the query, the format of
            the serialization, the destination directory where the files are serialized, and a list of any errors that
            occurred as tuples with the PID and the http status code.

        Examples:
            >>> Set('http://localhost:8080', yaml.safe_load(open("config.yml", "r"))).write_datastream_history('TN',
            ... 'xml')
            {'Attempted Downloads': ['test:4', 'test:5', 'test:6'], 'dsid': 'TN', 'format': 'xml', 'errors': [],
            'destination_directory': 'output'}

        """
        if self.settings["destination_directory"] in os.listdir("."):
            pass
        else:
            os.mkdir(self.settings["destination_directory"])
        errors = []
        for result in tqdm(self.results):
            r = requests.get(f"{self.settings['fedora_path']}:{self.settings['port']}/fedora/objects/{result}/"
                             f"datastreams/{dsid}/history?format={result_format}",
                             auth=(self.settings['username'], self.settings['password']))
            if r.status_code == 200:
                new_name = result.replace(":", "_")
                ext = r.headers["Content-Type"].split(";")[0].split("/")[1]
                with open(f"{self.settings['destination_directory']}/{new_name}.{ext}", "w") as new_file:
                    new_file.write(r.text)
            else:
                errors.append((result, r.status_code))
        return {"Attempted Downloads": self.results, "dsid": dsid, "format": result_format, "errors": errors,
                "destination_directory": self.settings['destination_directory']}

    def get_datastream_at_date(self, dsid, a_date="yyyy-MM-dd"):
        """Serializes to disk a datastream for all results at a specific date.

        Requires a datastream id (dsid) and a data (a_date) and downloads that version of the datastream for all
        matching results.

        Args:
            dsid (str): The datastream id that you want to serialize to disk.
            a_date (str): The date that you want to request as yyyy-MM-dd.

        Returns:
            dict: A dict with the PIDs attempted to download, the total number of pids attempted to download, the
            datastream id, the date that was requested, the destination directory where any successful downloads
            were serialized, and a list of errors as tuples containing the PID and the http status code associated
            with the request.

        Examples:
            >>> Set('http://localhost:8080', yaml.safe_load(open("config.yml", "r"))).get_datastream_at_date('MODS',
            ... '2019-11-12')
            {'Attempted downloads': ['test:4', 'test:5', 'test:6'], 'Downloads attempted': 3, 'dsid': 'MODS',
            'date requested': '2019-11-12', 'errors': [], 'destination_directory': 'output'}

        """
        if self.settings["destination_directory"] in os.listdir("."):
            pass
        else:
            os.mkdir(self.settings["destination_directory"])
        errors = []
        for result in tqdm(self.results):
            r = requests.get(f"{self.settings['fedora_path']}:{self.settings['port']}/fedora/objects/{result}/"
                             f"datastreams/{dsid}/content?asOfDateTime={a_date}",
                             auth=(self.settings['username'], self.settings['password']))
            if r.status_code == 200:
                new_name = result.replace(":", "_")
                ext = r.headers["Content-Type"].split(";")[0].split("/")[1]
                with open(f"{self.settings['destination_directory']}/{new_name}.{ext}", 'wb') as other:
                    other.write(r.content)
            else:
                errors.append((result, r.status_code))
        return {"Attempted downloads": self.results, "Downloads attempted": len(self.results), "dsid": dsid,
                "date requested": a_date, "errors": errors,
                "destination_directory": self.settings['destination_directory']}

    def write_all_versions_of_datastream(self, dsid):
        """Serializes all versions of a datastream related to a query to disk.

        Requires a datastream id (dsid) and serializes all versions of that datastream id related to a query to disk.
        Files are named in this pattern:  PID_DATE.EXTENSION

        Args:
            dsid (str): the datastream id you want to serialize to disk.

        Returns:
            dict: A dict with the PIDs attempted to download, the count of the PIDs attempted to download, a list of the
            files successfully serialized to disk, a list of errors as tuples with the PID and the http status code,
            and the destination directory where the files were serialized.

        Examples:
            >>> Set('http://localhost:8080', yaml.safe_load(open("config.yml", "r"))).write_all_versions_of_datastream(
            ... 'MODS')
            {'Attempted downloads': ['test:4', 'test:5', 'test:6'], 'PIDs attempted': 3, 'dsid': 'MODS',
            'serialized_files': ['test_4_2019-11-11T21:58:57.741Z.xml', 'test_4_2019-11-05T20:06:31.399Z.xml',
            'test_4_2019-11-05T17:49:30.565Z.xml', 'test_5_2019-11-13T14:39:04.198Z.xml',
            'test_5_2019-11-08T16:43:53.803Z.xml', 'test_5_2019-11-05T19:17:22.572Z.xml',
            'test_6_2019-11-05T19:17:56.183Z.xml', 'test_6_2019-11-05T19:17:56.183Z.xml',
            'test_6_2019-11-05T19:17:56.183Z.xml', 'test_6_2019-11-05T19:17:56.183Z.xml',
            'test_6_2019-11-05T19:17:56.183Z.xml', 'test_6_2019-11-05T19:17:56.183Z.xml',
            'test_6_2019-11-05T19:17:56.183Z.xml', 'test_6_2019-11-05T19:17:56.183Z.xml',
            'test_6_2019-11-05T19:17:56.183Z.xml', 'test_6_2019-11-05T19:17:56.183Z.xml',
            'test_6_2019-11-05T19:17:56.183Z.xml', 'test_6_2019-11-05T19:17:56.183Z.xml',
            'test_6_2019-11-05T19:17:56.183Z.xml', 'test_6_2019-11-05T19:17:56.183Z.xml',
            'test_6_2019-11-05T19:17:56.183Z.xml', 'test_6_2019-11-05T19:17:56.183Z.xml'], 'errors': [],
            'destination_directory': 'output'}

        """
        if self.settings["destination_directory"] in os.listdir("."):
            pass
        else:
            os.mkdir(self.settings["destination_directory"])
        errors = []
        serialized_files = []
        for result in tqdm(self.results):
            r = requests.get(f"{self.settings['fedora_path']}:{self.settings['port']}/fedora/objects/{result}/"
                             f"datastreams/{dsid}/history?format=xml",
                             auth=(self.settings['username'], self.settings['password']))
            if r.status_code == 200:
                json_response = json.loads(json.dumps(xmltodict.parse(r.text)['datastreamHistory']))
                for version in json_response['datastreamProfile']:
                    if type(version) is dict:
                        version_title = version['dsCreateDate']
                        current_version = requests.get(f"{self.settings['fedora_path']}:{self.settings['port']}/fedora/"
                                                       f"objects/{result}/datastreams/{dsid}/content?asOfDateTime="
                                                       f"{version['dsCreateDate']}",
                                                       auth=(self.settings['username'], self.settings['password']))
                        if current_version.status_code == 200:
                            new_name = result.replace(":", "_")
                            ext = current_version.headers["Content-Type"].split(";")[0].split("/")[1]
                            with open(f"{self.settings['destination_directory']}/{new_name}_{version_title}.{ext}", 'wb') as other:
                                other.write(current_version.content)
                            serialized_files.append(f'{new_name}_{version_title}.{ext}')
                        else:
                            errors.append((f'{result}_{version_title}.'
                                           f'{current_version.headers["Content-Type"].split(";")[0].split("/")[1]}',
                                           r.status_code))
                    elif type(version) is str:
                        version_title = json_response['datastreamProfile']['dsCreateDate']
                        current_version = requests.get(f"{self.settings['fedora_path']}:{self.settings['port']}/fedora/"
                                                       f"objects/{result}/datastreams/{dsid}/content?asOfDateTime="
                                                       f"{json_response['datastreamProfile']['dsCreateDate']}",
                                                       auth=(self.settings['username'], self.settings['password']))
                        if current_version.status_code == 200:
                            new_name = result.replace(":", "_")
                            ext = current_version.headers["Content-Type"].split(";")[0].split("/")[1]
                            with open(f"{self.settings['destination_directory']}/{new_name}_{version_title}.{ext}", 'wb') as other:
                                other.write(current_version.content)
                            serialized_files.append(f'{new_name}_{version_title}.{ext}')
                        else:
                            errors.append((f'{result}_{version_title}.'
                                           f'{current_version.headers["Content-Type"].split(";")[0].split("/")[1]}',
                                           r.status_code))
        return {"Attempted downloads": self.results, "PIDs attempted": len(self.results), "dsid": dsid,
                "serialized_files": serialized_files, "errors": errors,
                "destination_directory": self.settings['destination_directory']}

    def size_of_set(self):
        """Returns the total number of results in a query.

        Returns:
            int: the number of results in a query

        Examples:
            >>> Set('http://localhost:8080', yaml.safe_load(open("config.yml", "r"))).size_of_set()
            3

        """
        return len(self.results)

    def update_gsearch(self):
        successes = 0
        print("\n\nUpdating gsearch\n")
        with open("gsearch_log.txt", "w") as my_log:
            for result in tqdm(self.results):
                r = requests.post(f"{self.settings['fedora_path']}:{self.settings['port']}/fedoragsearch/rest?"
                                  f"operation=updateIndex&action=fromPid&value={result}",
                                  auth=(self.settings["gsearch_username"], self.settings["gsearch_password"]))
                if r.status_code == 200:
                    soup = BeautifulSoup(r.text, features="lxml")
                    success = False
                    for tag in soup.find_all("td"):
                        if tag.contents[0] == "Updated number of index documents: 1":
                            success = True
                    if success is True:
                        successes += 1
                        my_log.write(f"Successfully updated Solr document for {result}.\n")
                    else:
                        my_log.write(f"Failed to update Solr document for {result}.\n")
                else:
                    my_log.write(f"Failed to update Solr document for {result} with {r.status_code}.\n")
        print(f"\nSuccessfully updated {successes} records.")
        return

    def mark_as_missing(self, dsid=None):
        print(f"Finding results that are missing a {dsid} datastream.")
        missing = []
        for i in tqdm(self.results):
            r = requests.get(f"{self.settings['fedora_path']}:{self.settings['port']}/fedora/"
                             f"objects/{i}/datastreams/{dsid}", auth=(f"{self.settings['username']}",
                                                                      f"{self.settings['password']}"))
            if r.status_code != 200:
                missing.append(i)
        print(f"{len(missing)} of {len(self.results)} were missing a {dsid} datastream.")
        return missing

    def get_relationships(self):
        for i in tqdm(self.results):
            r = requests.get(f"{self.settings['fedora_path']}:{self.settings['port']}/fedora/"
                             f"objects/{i}/relationships", auth=(f"{self.settings['username']}",
                                                                 f"{self.settings['password']}"))
            if r.status_code == 200:
                print(r.text)
        return

    def find_rels_ext_relationship(self, relationship):
        membership_list = []
        print(f"Finding {relationship} objects for items in result list.")
        for i in tqdm(self.results):
            predicate = "&predicate=info:fedora/fedora-system:def/relations-external#" \
                        f"{relationship}".replace(":", "%3a").replace("/", "%2f").replace("#", "%23")
            r = requests.get(f"{self.settings['fedora_path']}:{self.settings['port']}/fedora/"
                             f"objects/{i}/relationships?subject=info%3afedora%2f{i}&format=turtle{predicate}",
                             auth=(f"{self.settings['username']}", f"{self.settings['password']}"))
            if r.status_code == 200:
                new_list = r.text.split(">")
                if len(new_list) == 4:
                    new_record = Record(i)
                    page_number = new_record.find_islandora_relationship("isPageNumber")
                    new_item = {"pid": i,
                                f"{relationship}": new_list[2].replace("<info:fedora/", "").replace(" ", ""),
                                "page number": page_number}
                    membership_list.append(new_item)
        return membership_list

    def list_dsids(self):
        unique_dsids = []
        for result in tqdm(self.results):
            url = f"{self.settings['fedora_path']}:{self.settings['port']}/fedora/objects/{result}/" \
                  f"datastreams?profiles=true"
            r = requests.get(url, auth=(self.settings["gsearch_username"], self.settings["gsearch_password"]))
            if r.status_code == 200:
                object_datastreams = json.loads(json.dumps(xmltodict.parse(r.text)))
                for object_datastream in object_datastreams['objectDatastreams']['datastreamProfile']:
                    if object_datastream['@dsID'] not in unique_dsids:
                        unique_dsids.append(object_datastream['@dsID'])
            else:
                print(r.status_code)
        print(f'The following unique dsids were found in your query: \n{unique_dsids}')
        return

    def get_datastream_report(self):
        """Prints a report on each datastream in a query.

        Prints a dict that includes how many times each dictionary is found and what pids it is associated with.

        Args:
            None

        Returns:
            None

        Examples:
            >>> Set('http://localhost:8080', yaml.safe_load(open("config.yml", "r"))).get_datastream_report()
            {'RELS-EXT': {'count': 3, 'pids': ['test:4', 'test:5', 'test:6']}, 'MODS': {'count': 3, 'pids': ['test:4',
            'test:5', 'test:6']}, 'DC': {'count': 3, 'pids': ['test:4', 'test:5', 'test:6']}, 'OBJ': {'count': 2,
            'pids': ['test:4', 'test:6']}, 'TECHMD': {'count': 2, 'pids': ['test:4', 'test:6']}, 'TN': {'count': 3,
            'pids': ['test:4', 'test:5', 'test:6']}, 'MEDIUM_SIZE': {'count': 2, 'pids': ['test:4', 'test:6']}}

        """
        unique_datastreams = {}
        for result in tqdm(self.results):
            url = f"{self.settings['fedora_path']}:{self.settings['port']}/fedora/objects/{result}/" \
                  f"datastreams?profiles=true"
            r = requests.get(url, auth=(self.settings["gsearch_username"], self.settings["gsearch_password"]))
            if r.status_code == 200:
                object_datastreams = json.loads(json.dumps(xmltodict.parse(r.text)))
                for object_datastream in object_datastreams['objectDatastreams']['datastreamProfile']:
                    if object_datastream['@dsID'] not in unique_datastreams.keys():
                        unique_datastreams[object_datastream['@dsID']] = {'count': 1, 'pids': [object_datastream['@pid']]}
                    else:
                        unique_datastreams[object_datastream['@dsID']]['count'] += 1
                        unique_datastreams[object_datastream['@dsID']]['pids'].append(object_datastream['@pid'])
        print(unique_datastreams)
        return

    def grab_foxml(self):
        for result in self.results:
            new_record = Record(result)
            foxml = new_record.grab_foxml()
            try:
                with open(f"{self.settings['destination_directory']}/{result}.xml", "w") as new_file:
                    new_file.write(foxml)
            except:
                pass
        return

    def test_embargos(self):
        for result in self.results:
            new_record = Record(result)
            new_record.am_i_embargoed()
        return

    def check_obj_mime_types(self):
        mime_types = {}
        for result in tqdm(self.results):
            new_record = Record(result)
            x = new_record.get_mime_type_of_object()
            if x is None:
                pass
            elif x not in mime_types:
                mime_types[x] = 1
            else:
                mime_types[x] += 1
        return mime_types

    def purge_all_but_newest_dsid(self, datastream):
        user_input = input(f"\n\nAre you sure you want to delete all but the newest {datastream} for each object in "
                           f"the collection? [y/N] ")
        if user_input == "y":
            with open(self.settings["log_file"], "w") as log_file:
                for result in tqdm(self.results):
                    new_record = Record(result)
                    dates = new_record.determine_old_dsid_versions(datastream)
                    if type(dates) is dict:
                        response = new_record.purge_old_dsid_versions(datastream, dates["start"], dates["end"])
                        log_file.write(response)
            return
        else:
            print("\nExiting...")
            return

    def write_results_to_file(self):
        with open("results.txt", 'w') as my_results:
            print("\nWriting results to results.txt.\n")
            for result in self.results:
                my_results.write(f"{result}\n")
            print("Done")
        return


class Record:
    def __init__(self, pid):
        self.pid = pid
        self.settings = yaml.safe_load(open("config.yml", "r"))

    def __repr__(self):
        return f"Record representing PID {self.pid}."

    def __str__(self):
        return f"Record representing PID {self.pid}."

    def find_islandora_relationship(self, relationship):
        predicate = "&predicate=http://islandora.ca/ontology/relsext#" \
                    f"{relationship}".replace(":", "%3a").replace("/", "%2f").replace("#", "%23")
        r = requests.get(f"{self.settings['fedora_path']}:{self.settings['port']}/fedora/objects/"
                         f"{self.pid}/relationships?subject=info%3afedora%2f{self.pid}&format=turtle{predicate}",
                         auth=(f"{self.settings['username']}", f"{self.settings['password']}"))
        if r.status_code == 200:
            new_list = r.text.split(' ')
            if len(new_list) is 4:
                page_number = new_list[2].replace('"', "")
                return page_number
        return

    def update_fgs_label(self, xpath="", page=None):
        if page is None:
            mods_path = f"{self.settings['islandora_path']}/islandora/object/{self.pid}/datastream/MODS/"
            opener = urllib.request.build_opener()
            document = etree.parse(opener.open(mods_path))
            label_path = document.xpath(xpath, namespaces={"mods": "http://www.loc.gov/mods/v3"})
            if len(label_path) > 0:
                print(f"Changing fgslabel for {self.pid} to {label_path[0].text}.")
                r = requests.put(f"{self.settings['fedora_path']}:{self.settings['port']}/fedora/objects/{self.pid}?"
                                 f"label={label_path[0].text}",
                                 auth=(self.settings['username'], self.settings['password']))
                if r.status_code == 200:
                    print(f"\tSuccessfully updated {self.pid}")
                else:
                    print(f"Failed to update with {r.status_code}.")
            else:
                print(f"Could not update.  Xpath did not match text for {self.pid}.")
        else:
            r = requests.put(f"{self.settings['fedora_path']}:{self.settings['port']}/fedora/objects/{self.pid}?"
                             f"label={page}",
                             auth=(self.settings['username'], self.settings['password']))
            if r.status_code == 200:
                print(f"\tSuccessfully updated {self.pid} to {page}.")
            else:
                print(f"Failed to update with {r.status_code}.")
        return

    def find_rels_ext_relationship(self, relationship):
        predicate = "&predicate=info:fedora/fedora-system:def/relations-external#" \
                    f"{relationship}".replace(":", "%3a").replace("/", "%2f").replace("#", "%23")
        r = requests.get(f"{self.settings['fedora_path']}:{self.settings['port']}/fedora/objects/"
                         f"{self.pid}/relationships?subject=info%3afedora%2f{self.pid}&format=turtle{predicate}",
                         auth=(f"{self.settings['username']}", f"{self.settings['password']}"))
        if r.status_code == 200:
            new_list = r.text.split(">")
            if len(new_list) == 4:
                page_number = self.find_islandora_relationship("isPageNumber")
                new_item = {"pid": self.pid,
                            f"{relationship}": new_list[2].replace("<info:fedora/", "").replace(" ", ""),
                            "page number": page_number}
                return new_item
        return

    def get_parent_label(self, xpath):
        mods = requests.get(f"{self.settings['fedora_path']}:{self.settings['port']}/fedora/objects/{self.pid}/"
                            f"datastreams/MODS/content", auth=(self.settings['username'], self.settings['password']))
        document = etree.fromstring(mods.content)
        label_path = document.xpath(xpath, namespaces={"mods": "http://www.loc.gov/mods/v3"})
        return label_path[0].text

    def grab_foxml(self, foxml_contents=None):
        r = requests.get(f"{self.settings['fedora_path']}:{self.settings['port']}/fedora/objects/{self.pid}/export",
                         auth=(f"{self.settings['username']}", f"{self.settings['password']}"))
        if r.status_code == 200:
            foxml_contents = r.text
        return foxml_contents

    def am_i_embargoed(self):
        r = requests.get(f"{self.settings['fedora_path']}:{self.settings['port']}/fedora/objects/{self.pid}/"
                         f"datastreams/RELS-INT", auth=(self.settings['username'], self.settings['password']))
        if r.status_code != 404:
            print(f"{self.pid}:  {r.status_code}")
        else:
            pass
        return

    def get_mime_type_of_object(self):
        r = requests.get(f"{self.settings['fedora_path']}:{self.settings['port']}/fedora/objects/{self.pid}/"
                         f"datastreams/OBJ/content", auth=(self.settings['username'], self.settings['password']))
        if r.status_code == 200:
            return r.headers['content-type']
        else:
            return None

    def determine_old_dsid_versions(self, dsid):
        r = requests.get(f"{self.settings['fedora_path']}:8080/fedora/objects/{self.pid}/datastreams/{dsid}/history?"
                         f"format=xml", auth=(self.settings['username'], self.settings['password']))
        if r.status_code == 200:
            response_text = xmltodict.parse(r.text)
            versions = []
            if type(response_text['datastreamHistory']['datastreamProfile']) == collections.OrderedDict:
                return "Don't Delete"
            else:
                for version in response_text['datastreamHistory']['datastreamProfile']:
                    versions.append(version["dsCreateDate"])
                versions.sort(reverse=True)
                if len(versions) >= 2:
                    return {"start": versions[-1], "end": versions[1]}
                else:
                    return "Only 1 version"

    def purge_old_dsid_versions(self, dsid, start=None, end=None):
        other_parameters = ""
        log_message = f"Purging {dsid} on {self.pid}"
        if start is not None:
            other_parameters += f"&startDT={start}"
            log_message += f" from {start}"
        if end is not None:
            other_parameters += f"&endDT={end}"
            log_message += f" until {end}"
        temp_request = f"{self.settings['fedora_path']}:8080/fedora/objects/{self.pid}/datastreams/{dsid}?" \
                       f"{other_parameters}{log_message}"
        r = requests.delete(f"{self.settings['fedora_path']}:8080/fedora/objects/{self.pid}/datastreams/{dsid}?"
                            f"{other_parameters}&logMessage={log_message}", auth=(self.settings['username'],
                                                                                  self.settings['password']))
        if r.status_code == 200:
            return log_message
        else:
            return f"Failed to purge {dsid} on {self.pid} with {r.status_code}.\n\n{temp_request}"

    def find_content_type(self):
        content_type = ""
        predicate = "&predicate=info:fedora/fedora-system:def/model#" \
                    "hasModel".replace(":", "%3a").replace("/", "%2f").replace("#", "%23")
        r = requests.get(f"{self.settings['fedora_path']}:{self.settings['port']}/fedora/objects/"
                         f"{self.pid}/relationships?subject=info%3afedora%2f{self.pid}&format=turtle{predicate}",
                         auth=(f"{self.settings['username']}", f"{self.settings['password']}"))
        for result in r.text.split(" "):
            if result.startswith("<info:fedora/islandora:"):
                content_type = result.replace("<info:fedora/islandora:", "").replace(">", "")
        return content_type
