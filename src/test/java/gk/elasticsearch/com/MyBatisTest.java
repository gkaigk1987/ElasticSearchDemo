package gk.elasticsearch.com;

import java.io.IOException;
import java.io.Reader;

import org.apache.ibatis.io.Resources;
import org.apache.ibatis.session.SqlSession;
import org.apache.ibatis.session.SqlSessionFactory;
import org.apache.ibatis.session.SqlSessionFactoryBuilder;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gk.elasticsearch.com.mapper.ThesisMapper;
import gk.elasticsearch.com.model.Thesis;

public class MyBatisTest {
	
	private static Logger logger = LoggerFactory.getLogger(MyBatisTest.class);
	
	private SqlSession sqlSession = null;
	
	@Before
	public void init() throws IOException {
		Reader resourceAsReader = Resources.getResourceAsReader("SqlMapConfig.xml");
		SqlSessionFactory sqlSessionFactory = new SqlSessionFactoryBuilder().build(resourceAsReader);
		sqlSession = sqlSessionFactory.openSession();
	}
	
	@After
	public void destroy() {
		if(sqlSession != null) {
			sqlSession.close();
		}
	}
	
	@Test
	public void test001() {
		ThesisMapper thesisMapper = sqlSession.getMapper(ThesisMapper.class);
		Thesis thesis = thesisMapper.selectByPrimaryKey(271958l);
		System.out.println(thesis.getThesisTitle());
	}

	@Test
	public void testLog() {
		logger.info("Log 测试");
	}
}
