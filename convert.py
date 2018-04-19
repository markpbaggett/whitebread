from subprocess import call
import yaml
import os

class Image:
    def __init__(self, name):
        self.name = name

    def convert(self, colorspace="sRGB", resize="200x200>", extension=".png"):
        output = self.name.split('.', maxsplit=1)[0]
        print(f"Converting {self.name} to {output}{extension}.")
        call(f"convert {self.name} -colorspace {colorspace} -resize '{resize}' {output}{extension}", shell=True)

def main():
    settings = yaml.load(open("config.yml", "r"))
    for path in os.walk(settings["destination_directory"]):
        for file in path[2]:
            img = Image(f"{settings['destination_directory']}/{file}")
            img.convert()

if __name__ == "__main__":
    main()