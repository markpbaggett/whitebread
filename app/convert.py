from subprocess import call
import yaml
import os
import argparse


class Image:
    def __init__(self, name):
        self.name = name

    def convert(self, colorspace="sRGB", resize="200x200>", extension=".png"):
        output = self.name.split('.', maxsplit=1)[0]
        print(f"Converting {self.name} to {output}{extension}.")
        call(f"convert {self.name} -colorspace {colorspace} -resize '{resize}' {output}{extension}", shell=True)

    def pdf_to_thumb(self, extension=".png"):
        output = self.name.split('.', maxsplit=1)[0]
        print(f"Converting {self.name} to {output}{extension}.")
        call(f"convert -thumbnail x250 -alpha remove '{self.name}[0]' {output}_TN.jpg", shell=True)

    def preview_to_thumb(self, extension=".png"):
        output = self.name.split('.', maxsplit=1)[0]
        print(f"Converting {self.name} to {output}{extension}.")
        call(f"convert -thumbnail x600 -alpha remove '{self.name}[0]' {output}_PREVIEW.jpg", shell=True)


def main():
    parser = argparse.ArgumentParser(description='Specify operation')
    parser.add_argument("-o", "--operation", dest="operation", help="Choose one: pdf_preview, pdf_thumb, thumb",
                        default="thumb")
    args = parser.parse_args()
    settings = yaml.load(open("../config.yml", "r"))
    for path in os.walk(settings["destination_directory"]):
        for file in path[2]:
            img = Image(f"{settings['destination_directory']}/{file}")
            if args.operation == "pdf_thumb":
                img.pdf_to_thumb()
            elif args.operation == "pdf_preview":
                img.preview_to_thumb()
            else:
                img.convert()

if __name__ == "__main__":
    main()