FROM        debian:latest
LABEL       maintainer="Egor Dubrovin dubrovin.en@ya.ru"

ENV         LANG=C.UTF-8 LC_ALL=C.UTF-8
ENV         PATH /opt/conda/bin:$PATH
ENV         PYSPARK_VERSION='3.0.0'

RUN         apt-get update --fix-missing && \
            apt-get install -y wget bzip2 ca-certificates \
            libglib2.0-0 libxext6 libsm6 libxrender1 curl \
            grep sed dpkg

RUN         ANACONDA_VERSION="Anaconda3-2020.07-Linux-x86_64.sh" && \
            wget https://repo.anaconda.com/archive/${ANACONDA_VERSION} -O ~/anaconda.sh && \
            /bin/bash ~/anaconda.sh -b -p /opt/conda && \
            rm ~/anaconda.sh && \
            ln -s /opt/conda/etc/profile.d/conda.sh /etc/profile.d/conda.sh && \
            echo ". /opt/conda/etc/profile.d/conda.sh" >> ~/.bashrc && \
            echo "conda activate base" >> ~/.bashrc && \
            /opt/conda/bin/conda install python=3.7 anaconda=custom -y && \
            /opt/conda/bin/conda install jupyter -y && \
            pip install pyspark==${PYSPARK_VERSION}

RUN         apt-get install -y default-jre

RUN         TINI_VERSION=`curl https://github.com/krallin/tini/releases/latest | grep -o "/v.*\"" | sed 's:^..\(.*\).$:\1:'` && \
            wget https://github.com/krallin/tini/releases/download/v${TINI_VERSION}/tini_${TINI_VERSION}.deb -O ~/tini.deb && \
            dpkg -i ~/tini.deb && \
            rm ~/tini.deb && \
            apt-get clean

ENTRYPOINT  ["tini", "--"]

EXPOSE      8888
CMD         ["jupyter", "notebook", "--allow-root", "--notebook-dir=/opt/notebooks", "--ip=0.0.0.0", "--port=8888", "--no-browser", "--NotebookApp.token=''", "--NotebookApp.password=''"]
