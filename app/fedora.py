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

class Set:
    def __init__(self, search_string, yaml_settings):
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
        return len(self.results)

    def harvest_metadata(self, dsid=None):
        if dsid is None:
            dsid = self.settings["default_dsid"]
        if self.settings["destination_directory"] in os.listdir("."):
            pass
        else:
            os.mkdir(self.settings["destination_directory"])
        for result in tqdm(self.results):
            r = requests.get(f"{self.settings['fedora_path']}:{self.settings['port']}/fedora/objects/{result}/"
                             f"datastreams/{dsid}/content",
                             auth=(f"settings['username']", f"settings['password']"))
            r.encoding = "utf-8"
            if r.status_code == 200:
                new_name = result.replace(":", "_")
                ext = r.headers["Content-Type"].split(";")[0].split("/")[1]
                with open(f"{self.settings['destination_directory']}/{new_name}.{ext}", "w") as new_file:
                    new_file.write(r.text)
            else:
                print(f"Could not harvest metadata for {result}: {r.status_code}.")
        print(f"\n\nDownloaded {len(self.results)} {dsid} records.")
        return

    def find_content_types(self):
        content_types = []
        for result in tqdm(self.results):
            x = Record(result).find_content_type()
            if x not in content_types:
                content_types.append(x)
        print(content_types)
        return

    def grab_images(self, dsid=None):
        if self.settings["destination_directory"] in os.listdir("."):
            pass
        else:
            os.mkdir(self.settings["destination_directory"])
        if dsid is None:
            dsid = self.settings["default_dsid"]
        for result in tqdm(self.results):
            r = requests.get(f"{self.settings['fedora_path']}:{self.settings['port']}/fedora/objects/{result}/"
                             f"datastreams/{dsid}/content",
                             auth=(f"settings['username']", f"settings['password']"))
            ext = r.headers["Content-Type"].split(";")[0].split("/")[1]
            in_file = Image.open(BytesIO(r.content))
            new_name = result.replace(":", "_")
            in_file.save(f"{self.settings['destination_directory']}/{new_name}.{ext}")
        return

    def grab_other(self, dsid=None):
        if self.settings["destination_directory"] in os.listdir("."):
            pass
        else:
            os.mkdir(self.settings["destination_directory"])
        if dsid is None:
            dsid = self.settings["default_dsid"]
        for result in tqdm(self.results):
            r = requests.get(f"{self.settings['fedora_path']}:{self.settings['port']}/fedora/objects/{result}/"
                             f"datastreams/{dsid}/content",
                             auth=(f"settings['username']", f"settings['password']"))
            if r.status_code == 200:
                new_name = result.replace(":", "_")
                ext = r.headers["Content-Type"].split(";")[0].split("/")[1]
                with open(f"{self.settings['destination_directory']}/{new_name}.{ext}", 'wb') as other:
                    other.write(r.content)
            else:
                print(f"Failed to download {dsid} for {result} with {r.status_code}.")
        return

    def size_of_set(self):
        return f"Total records: {len(self.results)}"

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
        for result in tqdm(self.results):
            print(f"Finding dsids for {result}.\n")
            url = f"{self.settings['fedora_path']}:{self.settings['port']}/fedora/objects/{result}/" \
                  f"datastreams?profiles=true"
            r = requests.get(url, auth=(self.settings["gsearch_username"], self.settings["gsearch_password"]))
            if r.status_code == 200:
                print(r.text)
            else:
                print(r.status_code)
        return

    def grab_foxml(self):
        for result in self.results:
            new_record = Record(result)
            foxml = new_record.grab_foxml()
            with open(f"{self.settings['destination_directory']}/{result}.xml", "w") as new_file:
                new_file.write(foxml)
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
        user_input = input(f"\n\nAre you sure you want to delete all but the newest {datastream} for each object in the "
                           f"collection? [y/N] ")
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
        self.settings = yaml.load(open("config.yml", "r"))

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
            document = etree.parse(mods_path)
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
        mods_path = f"{self.settings['islandora_path']}/islandora/object/{self.pid}/datastream/MODS/"
        document = etree.parse(mods_path)
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
