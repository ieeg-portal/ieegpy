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
import uuid
from pennprov.connection.mprov_connection import MProvConnection
import pennprov.models
from pennprov.models.subgraph_template import SubgraphTemplate
from pennprov.models.node_info import NodeInfo
from pennprov.models.link_info import LinkInfo


class AnnotationActivity:
    """
    Represents a ProvDM Activity for a run of an annotator function.

    Attributes:
        annotator_name: The name of the annotator function.
        annotation_layer: The Annotation layer to which an annotation created by this
                          Activity should belong.
        activity_index: The index of this Activity in the stream to which it belongs.
        start_time_utc: The datetime this Activity started. In UTC zone.
        end_time_utc: The datetime this Activity ended. In UTC zone.
        name: The name of this Activity.
    """

    def __init__(self, annotator_name, annotation_layer, activity_index,
                 start_time_utc, end_time_utc):
        self.annotator_name = annotator_name
        self.annotation_layer = annotation_layer
        self.activity_index = activity_index
        self.start_time_utc = start_time_utc
        self.end_time_utc = end_time_utc
        self.name = '{0}.{1}'.format(self.annotator_name, self.activity_index)

    def get_token(self):
        """
        Returns a pennprov.QualifiedName id for this Activity.
        """
        return pennprov.QualifiedName(
            MProvListener.activity_namespace, self.name)

    def get_node(self):
        """
        Returns a pennprov.NodeModel for this Activity.
        """
        attributes = [pennprov.models.Attribute(
            name=MProvListener.activity_attr_name, value=self.name, type='STRING')]
        activity = pennprov.NodeModel(
            type='ACTIVITY', attributes=attributes,
            start_time=self.start_time_utc, end_time=self.end_time_utc)
        return activity


class MProvWriter:
    """
    Writes provenance to the MProv system.
    """

    def __init__(self, mprov_connection):
        self.mprov_connection = mprov_connection
        self.dataset_name_to_token = {}
        self.timeseries_id_to_token = {}

    def write_input_channel_entities(self, dataset, input_channel_labels):
        """
        Ensures Entities exist for the input channels and the containing dataset.

        :param dataset: the ieeg.dataset.Dataset being processed.
        :param input_channel_labels: a list of strings. The labels of the channels being processed.
        """
        self._ensure_dataset_entity(dataset, input_channel_labels)
        template = self._get_subgraph_template(len(input_channel_labels))
        self.mprov_connection.get_low_level_api().store_subgraph_template(
            self.mprov_connection.get_graph(), template)

    def _ensure_dataset_entity(self, dataset, input_channel_labels):
        """
        Stores a Collection for the given dataset to the ProvDm store if necessary.
        """
        dataset_token = self.dataset_name_to_token.get(dataset.name)
        if dataset_token:
            return dataset_token
        mprov = self.mprov_connection
        graph = mprov.get_graph()
        dataset_token = pennprov.QualifiedName(
            MProvListener.dataset_namespace, dataset.name)
        try:
            self.mprov_connection.get_low_level_api().get_provenance_data(
                resource=graph, token=dataset_token)
        except pennprov.rest.ApiException as api_error:
            if api_error.status != 404:
                raise api_error
            attributes = [pennprov.models.Attribute(
                name=MProvListener.dataset_attr_name, value=dataset.name, type='STRING')]
            entity = pennprov.NodeModel(
                type='COLLECTION', attributes=attributes)
            mprov.prov_dm_api.store_node(resource=graph,
                                         token=dataset_token, body=entity)
            for input_channel_label in input_channel_labels:
                tsd = dataset.get_time_series_details(input_channel_label)
                ts_token = self._ensure_timeseries_entity(tsd)
                membership = pennprov.RelationModel(
                    type='MEMBERSHIP', subject_id=dataset_token, object_id=ts_token, attributes=[])
                mprov.prov_dm_api.store_relation(
                    resource=graph, body=membership, label='hadMember')
        self.dataset_name_to_token[dataset.name] = dataset_token
        return dataset_token

    def _ensure_timeseries_entity(self, ts_details):
        """
        Stores an Entity for the given TimeSeriesDetails instance.
        """
        ts_token = self.timeseries_id_to_token.get(ts_details.portal_id)
        if ts_token:
            return ts_token
        mprov = self.mprov_connection
        graph = mprov.get_graph()
        prov_api = mprov.get_low_level_api()
        token = token = pennprov.QualifiedName(
            MProvListener.timeseries_namespace, ts_details.portal_id)
        try:
            prov_api.get_provenance_data(
                resource=graph, token=token)
        except pennprov.rest.ApiException as api_error:
            if api_error.status != 404:
                raise api_error
            attributes = [pennprov.models.Attribute(
                name=MProvListener.timeseries_attr_name,
                value=ts_details.channel_label,
                type='STRING')]
            entity = pennprov.NodeModel(
                type='ENTITY', attributes=attributes)
            mprov.prov_dm_api.store_node(resource=graph,
                                         token=token, body=entity)
        self.timeseries_id_to_token[ts_details.portal_id] = token
        return token

    def _get_window_name(self, window, activity):
        return '{0}.w.{1}'.format(activity.annotator_name, window.window_index)

    def write_widow_prov(self, window, activity, annotation):
        """
        Writes the window, activity, and output to the prov store.
        :param window: The ieeg.processing.Window
        :param activity: The ieeg.annotation_processing.AnnotationActivity
                         that used the window as input.
        :param annotation: The ieeg.dataset.Annotation output by the activity.
        """
        mprov = self.mprov_connection
        graph = mprov.get_graph()
        window_name = self._get_window_name(window, activity)
        window_token = pennprov.QualifiedName(MProvListener.window_namespace,
                                              window_name)
        window_attributes = [
            pennprov.models.Attribute(
                name=MProvListener.window_start_time_name,
                value=window.window_start_usec, type='LONG'),
            pennprov.models.Attribute(
                name=MProvListener.window_end_time_name,
                value=(window.window_start_usec + window.window_size_usec), type='LONG')
        ]
        window_entity = pennprov.NodeModel(
            type='COLLECTION', attributes=window_attributes)
        mprov.prov_dm_api.store_node(resource=graph,
                                     token=window_token, body=window_entity)
        for input_channel_label in window.input_channel_labels:
            tsd = window.dataset.get_time_series_details(input_channel_label)
            ts_token = self._ensure_timeseries_entity(tsd)
            membership = pennprov.RelationModel(
                type='MEMBERSHIP', subject_id=window_token, object_id=ts_token, attributes=[])
            mprov.prov_dm_api.store_relation(
                resource=graph, body=membership, label='hadMember')

        self._store_activity(window_token, activity)
        if annotation:
            self._store_annotation(activity, annotation)

    def _store_activity(self, window_token, activity):
        """
        Stores an Activity if necessary
        """
        mprov = self.mprov_connection
        graph = mprov.get_graph()
        activity_token = activity.get_token()
        activity_node = activity.get_node()
        mprov.prov_dm_api.store_node(resource=graph,
                                     token=activity_token, body=activity_node)
        usage = pennprov.RelationModel(
            type='USAGE', subject_id=activity_token, object_id=window_token, attributes=[])
        mprov.prov_dm_api.store_relation(
            resource=graph, body=usage, label='used'
        )
        return activity_token

    def _store_annotation(self, activity, annotation):
        """
        Stores the given annotation in the ProvDm store.
        """
        mprov = self.mprov_connection
        graph = mprov.get_graph()
        annotation_id = '{0}.ann.0'.format(activity.name)

        ann_token = pennprov.QualifiedName(MProvListener.annotation_namespace,
                                           annotation_id)

        attributes = self._get_annotation_attributes(annotation)
        ann_entity = pennprov.NodeModel(type='ENTITY', attributes=attributes)
        mprov.prov_dm_api.store_node(resource=graph,
                                     token=ann_token, body=ann_entity)

        activity_token = activity.get_token()
        generation = pennprov.RelationModel(
            type='GENERATION', subject_id=ann_token, object_id=activity_token, attributes=[])
        mprov.prov_dm_api.store_relation(
            resource=graph, body=generation, label='wasGeneratedBy'
        )

        return ann_token

    @staticmethod
    def _get_annotation_attributes(annotation):
        """
        Returns list of pennprov Attributes for the given annotation.
        """
        attributes = [
            pennprov.models.Attribute(
                name=MProvListener.annotator_name, value=annotation.annotator, type='STRING'),
            pennprov.models.Attribute(
                name=MProvListener.type_name, value=annotation.type, type='STRING'),
            pennprov.models.Attribute(
                name=MProvListener.description_name, value=annotation.description, type='STRING'),
            pennprov.models.Attribute(
                name=MProvListener.layer_name, value=annotation.layer, type='STRING'),
            pennprov.models.Attribute(
                name=MProvListener.start_time_name,
                value=annotation.start_time_offset_usec,
                type='LONG'),
            pennprov.models.Attribute(
                name=MProvListener.end_time_name,
                value=annotation.end_time_offset_usec,
                type='LONG')
        ]
        return attributes

    @staticmethod
    def _get_subgraph_template(input_count):

        # rank_0 should be the dataset Entity, but for the moment this confuses
        # the prov store because its links are a subset of the window's links.
        rank_1 = [NodeInfo(id='ts{0}'.format(
            i), type='ENTITY', use_since=False) for i in range(input_count)]
        rank_2 = [NodeInfo(id='window',
                           type='COLLECTION',
                           use_since=True)]
        rank_3 = [NodeInfo(id='annotating',
                           type='ACTIVITY',
                           use_since=True)]
        rank_4 = [NodeInfo(id='output',
                           type='ENTITY',
                           use_since=True,
                           optional=True)]
        ranks = [
            rank_1,
            rank_2,
            rank_3,
            rank_4
        ]
        window_id = rank_2[0].id
        activity_id = rank_3[0].id
        output_id = rank_4[0].id
        links = [LinkInfo(source_id=window_id,
                          target_id=d.id,
                          type='hadMember') for d in rank_1]
        links.extend([
            LinkInfo(
                source_id=activity_id,
                target_id=window_id,
                type='used'
            ),
            LinkInfo(source_id=output_id,
                     target_id=activity_id,
                     type='wasGeneratedBy'
                     )])
        order_by = [window_id]
        template = SubgraphTemplate(
            ranks=ranks, links=links, order_by=order_by)
        return template


class MProvListener:
    """
    A hook into the MProv system. If an instance is passed to ieeg.Session() through
    the mprov_listener keyword arg its methods will be called when the appropriate
    ieeg.Dataset method is called.
    """
    dataset_namespace = MProvConnection.namespace + '/dataset#'
    dataset_attr_name = pennprov.QualifiedName(
        namespace=dataset_namespace, local_part='name')

    timeseries_namespace = MProvConnection.namespace + '/timeseries#'
    timeseries_attr_name = pennprov.QualifiedName(
        namespace=timeseries_namespace, local_part='name')

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

    activity_namespace = MProvConnection.namespace + '/activity#'
    activity_attr_name = pennprov.QualifiedName(
        namespace=activity_namespace, local_part='name')

    window_namespace = MProvConnection.namespace + '/window#'
    window_start_time_name = pennprov.QualifiedName(
        namespace=window_namespace, local_part='start_time_offset_usec')
    window_end_time_name = pennprov.QualifiedName(
        namespace=window_namespace, local_part='end_time_offset_usec')

    def __init__(self, mprov_connection):
        self.mprov_connection = mprov_connection
        self.dataset_id_to_token = {}
        self.timeseries_id_to_token = {}
        self.activity_name_to_token = {}

    def on_open_dataset(self, dataset_name, dataset):
        """
        Called when ieeg.Session.open_dataset() is called.
        """
        dataset_id = dataset.snap_id
        token = self.dataset_id_to_token.get(dataset_id)
        if not token:
            token = self.ensure_dataset_entity(dataset_name, dataset)
            self.dataset_id_to_token[dataset_id] = token

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
        attributes = [
            pennprov.models.Attribute(
                name=self.annotator_name, value=annotation.annotator, type='STRING'),
            pennprov.models.Attribute(
                name=self.type_name, value=annotation.type, type='STRING'),
            pennprov.models.Attribute(
                name=self.description_name, value=annotation.description, type='STRING'),
            pennprov.models.Attribute(
                name=self.layer_name, value=annotation.layer, type='STRING')
        ]
        return attributes

    def ensure_dataset_entity(self, dataset_name, dataset):
        """
        Stores a Collection for the given dataset to the ProvDm store if necessary.
        """
        mprov = self.mprov_connection
        graph = mprov.get_graph()
        prov_api = mprov.get_low_level_api()
        token = token = pennprov.QualifiedName(
            self.dataset_namespace, dataset.snap_id)
        try:
            prov_api.get_provenance_data(
                resource=graph, token=token)
        except pennprov.rest.ApiException as api_error:
            if api_error.status != 404:
                raise api_error
            attributes = [pennprov.models.Attribute(
                name=self.dataset_attr_name, value=dataset_name, type='STRING')]
            entity = pennprov.NodeModel(
                type='COLLECTION', attributes=attributes)
            mprov.prov_dm_api.store_node(resource=graph,
                                         token=token, body=entity)
            for _, tsd in dataset.ts_details.items():
                ts_token = self.timeseries_id_to_token.get(tsd.portal_id)
                if not ts_token:
                    ts_token = self.ensure_timeseries_entity(tsd)
                    self.timeseries_id_to_token[tsd.portal_id] = ts_token
                membership = pennprov.RelationModel(
                    type='MEMBERSHIP', subject_id=token, object_id=ts_token, attributes=[])
                mprov.prov_dm_api.store_relation(
                    resource=graph, body=membership, label='hadMember')
        return token

    def ensure_timeseries_entity(self, ts_details):
        """
        Stores an Entity for the given TimeSeriesDetails instance.
        """
        mprov = self.mprov_connection
        graph = mprov.get_graph()
        prov_api = mprov.get_low_level_api()
        token = token = pennprov.QualifiedName(
            self.timeseries_namespace, ts_details.portal_id)
        try:
            prov_api.get_provenance_data(
                resource=graph, token=token)
        except pennprov.rest.ApiException as api_error:
            if api_error.status != 404:
                raise api_error
            attributes = [pennprov.models.Attribute(
                name=self.timeseries_attr_name, value=ts_details.channel_label, type='STRING')]
            entity = pennprov.NodeModel(
                type='ENTITY', attributes=attributes)
            mprov.prov_dm_api.store_node(resource=graph,
                                         token=token, body=entity)
        return token

    def ensure_activity(self, annotation):
        """
        Stores an Activity
        """
        mprov = self.mprov_connection
        graph = mprov.get_graph()
        prov_api = mprov.get_low_level_api()
        annotator = annotation.annotator
        activity_token = activity_token = pennprov.QualifiedName(
            self.activity_namespace, annotator)
        try:
            prov_api.get_provenance_data(
                resource=graph, token=activity_token)
        except pennprov.rest.ApiException as api_error:
            if api_error.status != 404:
                raise api_error
            attributes = [pennprov.models.Attribute(
                name=self.activity_attr_name, value=annotator, type='STRING')]
            activity = pennprov.NodeModel(
                type='ACTIVITY', attributes=attributes)
            mprov.prov_dm_api.store_node(resource=graph,
                                         token=activity_token, body=activity)
            dataset_token = self.dataset_id_to_token.get(
                annotation.parent.snap_id)
            usage = pennprov.RelationModel(
                type='USAGE', subject_id=activity_token, object_id=dataset_token, attributes=[])
            mprov.prov_dm_api.store_relation(
                resource=graph, body=usage, label='used'
            )
        return activity_token

    def store_annotation(self, annotation):
        """
        Stores the given annotation in the ProvDm store.
        """
        mprov = self.mprov_connection
        graph = mprov.get_graph()
        annotation_id = (str(uuid.uuid4()) + '.' +
                         annotation.layer + '.' + annotation.type)

        ts_coll_token = pennprov.QualifiedName(self.annotation_namespace,
                                               (annotation_id + '/annotated'))
        ts_coll_attributes = [
            pennprov.models.Attribute(
                name=self.start_time_name, value=annotation.start_time_offset_usec, type='LONG'),
            pennprov.models.Attribute(
                name=self.end_time_name, value=annotation.end_time_offset_usec, type='LONG')
        ]
        ts_coll_entity = pennprov.NodeModel(
            type='COLLECTION', attributes=ts_coll_attributes)
        mprov.prov_dm_api.store_node(resource=graph,
                                     token=ts_coll_token, body=ts_coll_entity)

        for tsd in annotation.annotated:
            ts_token = self.timeseries_id_to_token.get(tsd.portal_id)
            membership = pennprov.RelationModel(
                type='MEMBERSHIP', subject_id=ts_coll_token, object_id=ts_token, attributes=[])
            mprov.prov_dm_api.store_relation(
                resource=graph, body=membership, label='hadMember')

        ann_token = pennprov.QualifiedName(self.annotation_namespace,
                                           annotation_id)

        attributes = self.get_annotation_attributes(annotation)
        ann_entity = pennprov.NodeModel(type='ENTITY', attributes=attributes)
        mprov.prov_dm_api.store_node(resource=graph,
                                     token=ann_token, body=ann_entity)

        annotates = pennprov.RelationModel(
            type='ANNOTATED', subject_id=ts_coll_token, object_id=ann_token, attributes=[])
        mprov.prov_dm_api.store_relation(
            resource=graph, body=annotates, label='_annotated')

        annotator = annotation.annotator
        activity_token = self.activity_name_to_token.get(annotator)
        if activity_token is None:
            activity_token = self.ensure_activity(annotation)
            self.activity_name_to_token[annotator] = activity_token
        generation = pennprov.RelationModel(
            type='GENERATION', subject_id=ann_token, object_id=activity_token, attributes=[])
        mprov.prov_dm_api.store_relation(
            resource=graph, body=generation, label='wasGeneratedBy'
        )

        return ann_token
