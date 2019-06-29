# 基础镜像
FROM centos

# 时区设置
RUN /bin/cp /usr/share/zoneinfo/Asia/Shanghai /etc/localtime \
  && echo 'Asia/Shanghai' >/etc/timezone \

# gcc 编译
RUN yum update -y \
    && yum install -y https://centos7.iuscommunity.org/ius-release.rpm \
    && yum install -y python36u python36u-libs python36u-devel python36u-pip \
    && yum install -y which gcc \
    && yum install -y openldap-devel

# 安装上传服务相关
RUN yum install lrzsz -y \
    && yum install zip -y \
    && yum install unzip -y

# pipenv
RUN pip3.6 install pipenv
RUN ln -s /usr/bin/pip3.6 /bin/pip
RUN rm /usr/bin/python
RUN ln -s /usr/bin/python3.6 /usr/bin/python


# 拷贝 项目文件
COPY . /home/sync_services/

WORKDIR /home/sync_services

# 安装依赖
RUN pip install -r ./requirements.txt

#ENTRYPOINT python run.py
