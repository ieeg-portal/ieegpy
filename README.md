# ieeg
*A simple Python API for IEEG.org*

**Authors: Joost B. Wagenaar, Zachary G. Ives, John Frommeyer, University of Pennsylvania**

This package allow users to stream data from the IEEG.org platform to their local Python environment.  An early iteration was developed in 2015, and it was substantially updated in 2019 to add greater functionality and to make it compatible with "modern" Python versions.  

Supported environments:
* Python 2.7+
* Python 3.6+

## Getting Started

First, be sure you have signed up at [IEEG.org](www.ieeg.org) so you have an active account and password.

Install the dependencies:
```
pip install requests numpy pandas deprecation pennprov
```

You may then run:
```
python read_sample.py myid mypassword
```

where `myid` is your IEEG.org username (you can put it in double-quotes if it has a space) and `mypassword` is your IEEG.org password (again, in double-quotes if you have a space in it).

You'll see that the sample program makes a connection to IEEG.org, fetches details on the channels associated with a given dataset, and fetches and dumps some data.

*Optional*: To install the libraries from this repository into your Python path, you can:

```
python setup.py build
python setup.py install
```  
You will need to install the library to run the programs in the `example` directory.

## Major Functionality

### Session (ieeg.auth)

* `open_dataset`(name):  fetches the metadata for an IEEG dataset, by its unique ID.  Returns a `TimeSeriesDetails` object.
* `close_dataset`(ds):  closes the connection for an IEEG dataset associated with a `TimeSeriesDetails` object.

### TimeSeriesDetails (ieeg.dataset)

You may access any of the following variables:
* `acquisition`: Acquisition system (if stored for channel).
* `name`: Internal name if stored for channel (often `chan_name`).
* `channel_label`: IEEG.org channel label and unique ID for channel.
* `min_sample` and `max_sample`: minimum and maximum integer value for each channel.
* `voltage_conversion_factor`: factor to multiply each sample by, to get the actual voltage reading.
* `number_of_samples`: number of samples in the channel recording.
* `start_time`: "official" start time of the recording. For human data this is usually masked.

### Annotation (ieeg.dataset)

You may access any of the following variables:
* `parent`: The Dataset to which this Annotation belongs
* `portal_id`: The id of this Annotation
* `annotated`: List of TimeSeriesDetails annotated by this Annotation
* `annotator`: The creator of this Annotation
* `type`: The type
* `description`: The description
* `layer`: The layer
* `start_time_offset_usec`: The start time of this Annotations. In microseconds since the recording start.
* `end_time_offset_usec`: The end time of this Annotation. In microseconds since the recording start.

### Dataset (ieeg.dataset)

* `get_channel_labels()`: Returns an ordered list of channel labels
* `get_time_series_details(label)`: Returns a `TimeSeriesDetails` for the named channel
* `get_channel_indices(list_of_labels)`: Takes a list of channel labels, and returns a list of channel indices.
* `get_data(start_offset, duration, list_of_channels)`: Given a start offset (in usec) and a duration, read all of the corresponding samples for the channels specified in `list_of_channels`.  Note that the list is the *indices* of the channels, as opposed to their labels.  You can call `get_channel_indices` to convert from labels to indices.  The result is a 2D array with one column per channel, and one row per sample.  We assume all channels are sampled at the same rate.
* `get_dataframe(start_offset, duration, list_of_channels)`: Given a start offset (in usec) and a duration, read all of the corresponding samples for the channels specified in `list_of_channels`.  Note that the list is the *indices* of the channels, as opposed to their labels.  You can call `get_channel_indices` to convert from labels to indices.  The result is a Pandas Dataframe in which the columns are the (labeled) channels.
* `get_annotation_layers()`: Returns a dictionary mapping annotatation layer names to the number of annotations in that layer.
* `get_annotations(layer_name, start_offset_usecs=None, first_result=None, max_results=None)`: Returns a list of annotations from the given layer ordered by the annotations' `start_time_offset_usec` attribute. If `start_offset_usecs` is given, then only annotations with a `start_time_offset_usec` attribute greater than or equal to `start_offset_usecs` will be returned. If `first_result` and `max_results` are specified, then the list will contain at most `max_results` annotations starting with the annotation at the zero-based "index" `first_result`. Otherwise, all annotations in the layer will be returned.
