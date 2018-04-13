# Whitebread Fedora

Simple python scripts for those everyday, mundane Fedora tasks.

## Installing

## Harvest Metadata!

**By Matching Parent Namespace:**
```
>>> python run.py -p smhc -o harvest_metadata
```

**By Dublincore Field Matching:**

```
>>> python run.py -o harvest_metadata -dc rights -dcs "In Copyright"
```

**Override Default DSID without Touching YAML:**

```
>>> python run.py -o harvest_metadata -p smhc -ds DC
```

## Grab Images

** Like above, but for image mime types **
```
>>> python run.py -p smhc -o grab_images -ds JP2
```

## Download Binaries!

**Just like above examples but with a different operator (use for things that aren't images or test).**

```
>>> python run.py -o grab_other -p smhc -ds PDF
```

## Update GSearch!

**You guessed it!**

```
>>> python run.py -o update_gsearch -p smhc
```
