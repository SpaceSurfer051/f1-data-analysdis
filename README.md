# f1 data analysis

## Skills & Tech Stack

|skill|detail|
|---|---|
| **언어** | Python |
| **배포** | GKE |
| **대시보드** | Superset |
| **파이프라인** | Airflow |



## 프로젝트 목적
F1 통계 데이터를 통해 대시보드를 작성해 시즌별 레이스 결과,드라이버 성적,팀 순위,랩 타임 등을 분석



## 데이터 파이프라인 
<img src="https://github.com/SpaceSurfer051/f1-data-analysis/blob/main/img/f1_data_pipeline.png" width="1000" height="600"/> 
### dags
Task 1: 데이터 추출 및 GCS 업로드
- OPEN F1 API로 GET 요청을 보내 새로운 Race Session에 대한 데이터를 추출
- CSV 파일로 변환
- GCS(Google Cloud Storage)에 업로드
Task 2: GCS에서 BigQuery
- GCS에 저장한 csv 파일을 Google BigQuery 데이터 웨어하우스의 테이블로 적재


## 대시보드 화면
<img src="https://github.com/SpaceSurfer051/f1-data-analysis/blob/main/img/score_player_team.png" width="500"/>

### 선수별 점수
- 선수 별 현재까지 합산된 점수를 연도별로 구분하여 막대그래프로 시각화
- 2023, 2024 연도의 데이터를 비교하여 확인 가능
- 선수들 간의 격차와 순위 확인 가능

### 팀별 점수
- 팀 별 현재까지 합산된 점수를 연도별로 구분하여 막대그래프로 시각화
- 2023, 2024 연도의 데이터를 비교하여 확인 가능

<img src="https://github.com/SpaceSurfer051/f1-data-analysis/blob/main/img/all_game_result.png" width="500"/>

### 전체 경기 결과
- 각 경기 별 최종 결과를 리스팅 한 테이블

### 가장 빠른 LAP TIME TOP 10
- 각 경기 별 가장 빠른 Lap Time을 기록한 선수 10명을 리스팅한 테이블
- 선수 이름, 최종 결과와 함께 출력하여 Fastest Lap으로 부여되는 점수에 대한 정보 제공
- Fastest Lap: 가장 빠른 Lap Time을 기록한 선수가 10위 안에 들어오면 1점을 추가로 주는 제도
- 각 경기별 가장 빠른 선수와 최종 순위 상위권 선수를 비교해 Lap Time이 빠른 선수와 상위권인 선수들의 관계에 대한 통찰 제공

<img src="https://github.com/SpaceSurfer051/f1-data-analysis/blob/main/img/circuit.png" width="500"/>

### 전체 서킷 타이어 분포
- 일년동안 전체 경기에서 사용한 타이어의 비율

<img src="https://github.com/SpaceSurfer051/f1-data-analysis/blob/main/img/tire.png" width="500"/>

### 선수가 타이어별로 주행한 랩수
- 각 선수가 전체 경기에서 해당 타이어로 주행한 랩의 합계

### 서킷당 컴파운드별 수명
- 해당 서킷에서 모든 선수가 해당 타이어로 주행한 랩의 합계




