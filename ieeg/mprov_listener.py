'''
 Copyright 2019 Trustees of the University of Pennsylvania

 Licensed under the Apache License, Version 2.0 (the "License");
 you may not use this file except in compliance with the License.
 You may obtain a copy of the License at

 http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.
'''

from pennprov.metadata.stream_metadata import BasicSchema, BasicTuple


class MProvStream:
    """
    A MProv Stream
    """

    def __init__(self, stream_name, schema=None, initial_index=None):
        self.name = stream_name
        self.schema = BasicSchema(self.name) if schema is None else schema
        self._index = 1 if initial_index is None else initial_index

    def get_index(self):
        """
        Returns the current stream index.
        """
        index = self._index
        self._index += 1
        return index


class AnnotationStreamFactory:
    """
    Provides a MProvStream given an ieeg.dataset.Annotation instance.
    """

    def __init__(self, get_stream_name_fn=None):
        if get_stream_name_fn:
            self.get_stream_name = get_stream_name_fn
        else:
            self.get_stream_name = lambda ann: ann.parent.snap_id
        self.streams = {}

    def get_annotation_stream(self, annotation, initial_index=None):
        """
        Returns the stream corresponding to the given annotation.
        If no such stream exists, it is created.
        """
        stream_name = self.get_stream_name(annotation)
        stream = self.streams.get(stream_name)
        if not stream:
            stream = MProvStream(stream_name, initial_index)
            self.streams[stream_name] = stream

        return stream

    def get_annotation_name(self, annotation):
        """
        Returns the name of the given annotation.
        """
        return annotation.layer + '.' + annotation.type

    def get_annotation_value(self, annotation):
        """
        Returns the value of the given annotation.
        """
        return annotation.description


class MProvListener:
    """
    A hook into the MProv system. If an instance is passed to ieeg.Session() through
    the mprov_listener keyword arg its methods will be called when the appropriate
    ieeg.Dataset method is called.
    """

    def __init__(self, mprov_connection, annotation_stream_factory=None):
        self.mprov_connection = mprov_connection
        self.annotation_stream_factory = (
            annotation_stream_factory if annotation_stream_factory else AnnotationStreamFactory())

    def on_add_annotations(self, annotations):
        """
        Called when ieeg.Dataset.add_annotations() is called.
        """
        for annotation in annotations:
            stream = self.annotation_stream_factory.get_annotation_stream(
                annotation)
            stream_name = stream.name
            index = stream.get_index()
            schema = stream.schema
            mprov_tuple = BasicTuple(schema)
            self.mprov_connection.store_stream_tuple(
                stream_name, index, mprov_tuple)
            annotation_name = self.annotation_stream_factory.get_annotation_name(
                annotation)
            annotation_value = self.annotation_stream_factory.get_annotation_value(
                annotation)
            self.mprov_connection.store_annotation(stream_name,
                                                   index,
                                                   annotation_name,
                                                   annotation_value)
