<div align=center>
  <h1>🎮 Steamgame_Dashboard </h1>
</div>

<div align=center>
  <h3> Steam API를 활용한 Steam 게임 트렌드 및 유저 활동 분석</h3>
  <p> 실시간 Steam API, Steamspy API를 활용하여 약 140,000개 이상의 Steam 게임에 대한 상세 정보, 리뷰 데이터, 유저 활동 데이터를 수집하고 분석했다. <br> 이를 통해 다양한 흥미로운 인사이트를 추출하여 한눈에 볼 수 있는 다양한 차트와 그래프가 있는 대시 보드를 생성하는 데 중점을 두었으며, <br> 웹을 보는 사용자로 하여금 Steam 게임을 다양한 측면에서 비교하고, 탐색할 수 있는 환경을 제공하고자 했다. </p>
</div>
<br>

## 목차
- [프로젝트 개요](#✔️-프로젝트-개요)
- [Tech Stack](#⚒️-Tech-Stack)
- [Architecture](#🏛️-Architecture)
- [Data Pipeline](#🌈-Data-pipeline)
- [ERD](#🧩-ERD)
- [결과](#🎁-결과)

## ✔️ 프로젝트 개요
### 목적
- 전세계적으로 인기 있는 온라인 게임 유통 플랫폼인 Steam에서 제공하는 다양한 게임 데이터를 수집하고 분석하여 게임 업계의 동향과 트렌드를 파악한다.
- 수집된 데이터를 통해 게임 장르, 출시 시기, 가격 등과 인기의 상관 관계를 분석하여 인사이트를 제공한다.
- 분석 결과를 대시 보드로 시각화해 제공함으로써 데이터를 직관적으로 이해하고 비교할 수 있도록 한다. 

### 개발 기간
- 23.06.26 ~ 23.07.07 (2주)

### 팀원 소개 및 역할
|  이름  | 역할 | GitHub | 
| :---: | :---: | :--- |
| 김승언 | AWS 및 DBT 인프라 구축, DAG 작성(ELT), 시각화, 배포 | [@SikHyeya](https://github.com/SikHyeya) |
| 김선호 | AWS 및 DBT 인프라 구축, DAG 작성(ELT), 시각화, 배포 | [@sunhokim1457](https://github.com/sunhokim1457) |
| 김지석 | Steam API 데이터 수집, DAG 작성(ETL, ELT) 시각화, 배포 | [@plays0708](https://github.com/plays0708) |
| 송지혜 | Steam API 데이터 수집, DAG 작성(ETL, ELT) 시각화, 배포 | [@ssongjj](https://github.com/ssongjj) |
<br>
<br>

## ⚒️ Tech Stack
| Field | Stack |
|:---:|:---:|
| Database | <img src="https://img.shields.io/badge/snowflake-29B5E8?style=for-the-badge&logo=snowflake&logoColor=white"><img src="https://img.shields.io/badge/amazons3-569A31?style=for-the-badge&logo=amazons3&logoColor=white"> ||
| ETL & ELT | <img src="https://img.shields.io/badge/Docker-2496ED?style=for-the-badge&logo=docker&logoColor=white"/><img src="https://img.shields.io/badge/Airflow-017CEE?style=for-the-badge&logo=Apache%20Airflow&logoColor=white"/><img src="https://img.shields.io/badge/python-3776AB?style=for-the-badge&logo=python&logoColor=white"><img src="https://img.shields.io/badge/dbt-FF694B?style=for-the-badge&logo=dbt&logoColor=white"> ||
| Dashboard | <img src="https://img.shields.io/badge/tableau-E97627?style=for-the-badge&logo=tableau&logoColor=white"> ||
| Web | <img src="https://img.shields.io/badge/django-092E20?style=for-the-badge&logo=django&logoColor=white"> ||
| CI/CD | <img src="https://img.shields.io/badge/githubactions-2088FF?style=for-the-badge&logo=githubactions&logoColor=white"><img src="https://img.shields.io/badge/amazonec2-FF9900?style=for-the-badge&logo=amazonec2&logoColor=white">||
| Tools | <img src="https://img.shields.io/badge/github-181717?style=for-the-badge&logo=github&logoColor=white"><img src="https://img.shields.io/badge/figma-F24E1E?style=for-the-badge&logo=figma&logoColor=white"><img src="https://img.shields.io/badge/trello-0052CC?style=for-the-badge&logo=trello&logoColor=white">||
<br>
<br>


## Architecture
![image](https://github.com/data-engineering-team4/CorpAnalytica/assets/123959802/bdbae6b2-9ab0-4ffc-8593-b78c4e9b5312)
<br>
<br>

## 🌈 Data pipeline
![image](https://github.com/data-engineering-team4/CorpAnalytica/assets/123959802/76b2e609-c5a3-4596-87d0-feedd47ec977)
<br>
<br>

## 🧩 ERD
![image](https://github.com/data-engineering-team4/Steamgame_Dashboard/assets/39427152/c87931ea-6ba5-4795-a29e-53aff5b7bde0)
<br>
<br>

## 🎁 결과 
- 현재 태블로 클라우드의 무료 제공 기간 이슈로 EC2 배포한 내용은 내려 배포 당시 웹 페이지 전체 스크린샷으로 첨삭
<br>

![result](https://github.com/data-engineering-team4/CorpAnalytica/assets/123959802/da60437c-1fc9-4d7e-af6f-effba3a9fc7a)
<br>
