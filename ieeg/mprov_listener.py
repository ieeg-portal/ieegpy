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
