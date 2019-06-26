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

from pennprov.connection.mprov_connection import MProvConnection
from pennprov.metadata.stream_metadata import BasicSchema, BasicTuple
import pennprov.models


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


class AnnotationStreamFactory2:
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


class MProvListener2:
    """
    A hook into the MProv system. If an instance is passed to ieeg.Session() through
    the mprov_listener keyword arg its methods will be called when the appropriate
    ieeg.Dataset method is called.
    """
    annotation_namespace = MProvConnection.namespace + '/annotation#'
    annotator_name = pennprov.QualifiedName(
        namespace=annotation_namespace, local_part='annotator')
    type_name = pennprov.QualifiedName(
        namespace=annotation_namespace, local_part='type')
    description_name = pennprov.QualifiedName(
        namespace=annotation_namespace, local_part='description')
    layer_name = pennprov.QualifiedName(
        namespace=annotation_namespace, local_part='layer')
    start_time_name = pennprov.QualifiedName(
        namespace=annotation_namespace, local_part='start_time_offset_usec')
    end_time_name = pennprov.QualifiedName(
        namespace=annotation_namespace, local_part='end_time_offset_usec')
    annotated_name = pennprov.QualifiedName(
        namespace=annotation_namespace, local_part='annotated')

    def __init__(self, mprov_connection, annotation_stream_factory=None):
        self.mprov_connection = mprov_connection
        self.annotation_stream_factory = (
            annotation_stream_factory if annotation_stream_factory else AnnotationStreamFactory2())

    def on_add_annotations(self, annotations):
        """
        Called when ieeg.Dataset.add_annotations() is called.
        """
        for annotation in annotations:
            self.store_annotation(annotation)

    def get_annotation_attributes(self, annotation):
        """
        Returns list of pennprov Attributes for the given annotation.
        """
        attributes = []
        attributes.append(pennprov.models.Attribute(
            name=self.annotator_name, value=annotation.annotator, type='STRING'))
        attributes.append(pennprov.models.Attribute(
            name=self.type_name, value=annotation.type, type='STRING'))
        attributes.append(pennprov.models.Attribute(
            name=self.description_name, value=annotation.description, type='STRING'))
        attributes.append(pennprov.models.Attribute(
            name=self.layer_name, value=annotation.layer, type='STRING'))
        attributes.append(pennprov.models.Attribute(
            name=self.start_time_name, value=annotation.start_time_offset_usec, type='LONG'))
        attributes.append(pennprov.models.Attribute(
            name=self.end_time_name, value=annotation.end_time_offset_usec, type='LONG'))
        for tsd in annotation.annotated:
            attributes.append(pennprov.models.Attribute(
                name=self.annotated_name, value=tsd.channel_label, type='STRING'))

        return attributes

    def ensure_dataset_entity(self, token):
        """
        Stores the given dataset token to the ProvDm store if necessary.
        """
        mprov = self.mprov_connection
        graph = mprov.get_graph()
        prov_api = mprov.get_low_level_api()
        try:
            prov_api.get_provenance_data(
                resource=graph, token=token)
        except pennprov.rest.ApiException as api_error:
            if api_error.status != 404:
                raise api_error
            entity = pennprov.NodeModel(type='ENTITY', attributes=[])
            mprov.prov_dm_api.store_node(resource=graph,
                                         token=token, body=entity)

    def store_annotation(self, annotation):
        """
        Stores the given annotation in the ProvDm store.
        """
        mprov = self.mprov_connection
        stream = self.annotation_stream_factory.get_annotation_stream(
            annotation)
        stream_index = stream.get_index()

        # The "token" for the tuple will be the node ID
        token = pennprov.QualifiedName(
            mprov.namespace + '/dataset#', annotation.parent.snap_id)
        self.ensure_dataset_entity(token)

        ann_token = pennprov.QualifiedName(self.annotation_namespace,
                                           mprov.get_entity_id(stream.name +
                                                               '.' +
                                                               annotation.layer +
                                                               '.' +
                                                               annotation.type, stream_index))

        # The key/value pair will itself be an entity node
        attributes = self.get_annotation_attributes(annotation)
        entity_node = pennprov.NodeModel(type='ENTITY', attributes=attributes)
        mprov.prov_dm_api.store_node(resource=mprov.get_graph(),
                                     token=ann_token, body=entity_node)

        # Then we add a relationship edge (of type ANNOTATED)
        annotates = pennprov.RelationModel(
            type='ANNOTATED', subject_id=token, object_id=ann_token, attributes=[])
        mprov.prov_dm_api.store_relation(
            resource=mprov.get_graph(), body=annotates, label='_annotated')

        return ann_token
