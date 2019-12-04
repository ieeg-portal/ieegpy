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
import math as m


class AnnotatorContext:
    """
    Provides context to an annotator function.
    """

    def __init__(self,
                 dataset,
                 input_channel_labels,
                 annotation_layer,
                 window_start_usec,
                 window_size_usec):
        self.dataset = dataset
        self.input_channel_labels = input_channel_labels
        self.annotation_layer = annotation_layer
        self.window_start_usec = window_start_usec
        self.window_size_usec = window_size_usec


class SlidingWindowAnnotator:

    def __init__(self,
                 window_size_usec,
                 slide_usec,
                 annotator_function,
                 mprov_connection=None):
        self.window_size_usec = window_size_usec
        self.slide_usec = slide_usec
        self.annotator_function = annotator_function
        self.mprov_connection = mprov_connection

    def annotate_dataset(self,
                         dataset,
                         annotation_layer,
                         start_time_usec=None,
                         duration_usec=None,
                         input_channel_labels=None):
        if input_channel_labels is None:
            input_channel_labels = dataset.get_channel_labels()
        if start_time_usec is None:
            start_time_usec = 0
        if duration_usec is None:
            duration_usec = dataset.end_time - dataset.start_time

        input_channel_indices = dataset.get_channel_indices(
            input_channel_labels)
        if self.mprov_connection:
            self._write_input_channel_entities(input_channel_labels)

        annotations = []

        for window in range(0, int(m.ceil(duration_usec / self.slide_usec))):
            window_start_usec = start_time_usec + window * self.slide_usec
            annotation_context = AnnotatorContext(
                dataset,
                input_channel_labels,
                annotation_layer,
                window_start_usec,
                self.window_size_usec)
            data_block = dataset.get_data(window_start_usec,
                                          self.window_size_usec,
                                          input_channel_indices)
            new_annotations = self.annotator_function(
                data_block, annotation_context)

            annotations.extend(new_annotations)
            if self.mprov_connection:
                self._write_widow_prov()

        dataset.add_annotations(annotations)
        return annotations

    def _write_widow_prov(self):
        pass

    def _write_input_channel_entities(self, input_channel_labels):
        pass
