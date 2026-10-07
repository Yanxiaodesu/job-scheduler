# ============================================================
#  多阶段构建
#
#  为什么分两阶段：编译需要完整的 JDK + Maven（约 700MB），
#  运行只需要 JRE（约 180MB）。分阶段能让最终镜像小一大截。
# ============================================================

# ---------- 构建阶段 ----------
FROM maven:3.9-eclipse-temurin-17 AS build

WORKDIR /build

# 先只拷 pom.xml 单独拉依赖：这样只要 pom 没改，
# 改代码时这一层就能命中缓存，不用重新下几百 MB
COPY pom.xml .
COPY docker/maven-settings.xml /root/.m2/settings.xml
RUN mvn -B -q dependency:go-offline

COPY src ./src
RUN mvn -B -q package -DskipTests

# ---------- 运行阶段 ----------
FROM eclipse-temurin:17-jre

WORKDIR /app

# 时区：容器默认 UTC，不设的话 cron 会按 UTC 触发，差 8 小时
ENV TZ=Asia/Shanghai
RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone

COPY --from=build /build/target/job-scheduler-*.jar app.jar

# JVM 参数可以用 JAVA_OPTS 覆盖
ENV JAVA_OPTS="-Xms128m -Xmx512m"

EXPOSE 8080

ENTRYPOINT ["sh", "-c", "java $JAVA_OPTS -jar app.jar"]
