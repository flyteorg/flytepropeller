
�
2".core.containerization.multi_images.svm_trainerpython-task0.32.6python* "�
 �
)
test_labels
B	parquettest_labels
-
test_features
B	parquettest_features
+
train_labels
B	parquettrain_labels
/
train_features
B	parquettrain_features2�
qghcr.io/flyteorg/flytecookbook:core-with-sklearn-baa17ccf39aa667c5950bd713a4366ce7d5fccaf7f85e6be8c07fe4b522f92c3pyflyte-execute--inputs
{{.input}}--output-prefix{{.outputPrefix}}--raw-output-data-prefix{{.rawOutputDataPrefix}}--checkpoint-path{{.checkpointOutputPrefix}}--prev-checkpoint{{.prevCheckpointPrefix}}
--resolver9flytekit.core.python_auto_container.default_task_resolver--task-module"core.containerization.multi_images	task-namesvm_trainer" 