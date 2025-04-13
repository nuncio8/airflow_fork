#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
from __future__ import annotations

from datetime import datetime
 
from airflow.models.dag import DAG, DagModel
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.sdk.definitions.context import get_parsing_context
from airflow.utils.session import create_session
 
def test_dag_is_disabled_on_removal():
    DAG_ID = "test_dag_disable_on_removal"

    # Step 1: Create and add the DAG to the database
    with DAG(
        DAG_ID,
        start_date=datetime(2024, 2, 21),
        schedule="0 0 * * *",
    ) as test_dag:
        EmptyOperator(task_id="task_1")
    
    with create_session() as session:
        dag_mode#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
from __future__ import annotations

from datetime import datetime
 
from airflow.models.dag import DAG, DagModel
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.sdk.definitions.context import get_parsing_context
from airflow.utils.session import create_session
 
def test_dag_is_disabled_on_removal():
    DAG_ID = "test_dag_disable_on_removal"
 
    # Step 1: Create and add the DAG to the database
    with DAG(
         DAG_ID,
        start_date=datetime(2024, 2, 21),
        schedule="0 0 * * *",
    ) as test_dag:
        EmptyOperator(task_id="task_1")
     
    with create_session() as session:
        dag_model = DagModel(dag_id=DAG_ID, is_active=True)
        session.add(dag_model)
        session.commit()
     
    # Verify the DAG is active in the database
    with create_session() as session:
        dag_in_db = session.query(DagModel).filter(DagModel.dag_id == DAG_ID).one()
        assert dag_in_db.is_active is True
     
    # Step 2: Simulate DAG removal
    dag_op = DagModelOperation(bundle_name="test_bundle", bundle_version=None, dags={})
     
    with create_session() as session:
        dag_op.cleanup_unused_dags({}, session=session)
     
    # Verify the DAG is disabled
    with create_session() as session:
        dag_in_db = session.query(DagModel).filter(DagModel.dag_id == DAG_ID).one()
        assert dag_in_db.is_active is False
        session.add(dag_model)
        session.commit()
     
    # Verify the DAG is active in the database
    with create_session() as session:
        dag_in_db = session.query(DagModel).filter(DagModel.dag_id == DAG_ID).one()
        assert dag_in_db.is_active is True
     
    # Step 2: Simulate DAG removal
    dag_op = DagModelOperation(bundle_name="test_bundle", bundle_version=None, dags={})
     
    with create_session() as session:
        dag_op.cleanup_unused_dags({}, session=session)
     
    # Verify the DAG is disabled
    with create_session() as session:
        dag_in_db = session.query(DagModel).filter(DagModel.dag_id == DAG_ID).one()
        assert dag_in_db.is_active is False