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


class MProvStream:
    """
    A MProv Stream
    """

    def __init__(self, stream_name, initial_index):
        self.name = stream_name
        self.index = initial_index

    def get_index(self):
        """
        Returns the current stream index.
        """
        index = self.index
        self.index += 1
        return index


class AnnotationStream(MProvStream):
    """
    A MProvStream for ieeg.Annotation
    """

    def __init__(self, name='annotations', initial_index=0):
        super(AnnotationStream, self).__init__(self, name, initial_index)

    def get_annotation_name(self, annotation):
        """
        Returns the name of the given annotation.
        """
        return annotation.type

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

    def __init__(self, mprov_connection, annotation_stream=None):
        self.mprov_connection = mprov_connection
        self.annotation_stream = annotation_stream if annotation_stream else AnnotationStream()

    def on_add_annotations(self, annotations):
        """
        Called when ieeg.Dataset.add_annotations() is called.
        """
        for annotation in annotations:
            stream_name = 'annotations'
            annotation_name = self.annotation_stream.get_annotation_name(
                annotation)
            annotation_value = self.annotation_stream.get_annotation_value(
                annotation)
            self.mprov_connection.store_annotation(stream_name,
                                                   self.annotation_stream.get_index(),
                                                   annotation_name,
                                                   annotation_value)
