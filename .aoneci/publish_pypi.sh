#!/usr/bin/env sh
cd python
pwd
pip3 install poetry
poetry build

# 发布到测试
echo "is_test:${is_test}"
if [ "$is_test" == "true" ]; then
  poetry config repositories.testpypi https://test.pypi.org/legacy/
  rm -rf dist/
  poetry -vvv publish --build --repository testpypi --username "__token__" --password ${test_token} --no-interaction
fi

# 发布到正式
echo "is_prod:${is_prod}"
if [ "$is_prod" == "true" ]; then
  rm -rf dist/
  poetry -vvv publish --build --username "__token__" --password ${prod_token} --no-interaction
fi

