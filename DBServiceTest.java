package ru.bank.service;

import com.sonicsw.tools.esb.junit.ESBJUnitInitException;
import com.sonicsw.tools.esb.junit.MsgRequest;
import com.sonicsw.tools.esb.junit.impl.RequestFactory;
import com.sonicsw.xq.*;
import org.apache.log4j.Logger;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import ru.bank.esb.core.Serialise_lib1;
import ru.bank.test.XQInitContextImpl;
import ru.bank.test.XQServiceContextImpl;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.concurrent.*;

import javax.xml.xpath.*;
import java.io.IOException;
import java.io.InputStream;

public class DBServiceTest {

    private final static Logger log = Logger.getLogger(DBServiceTest.class);
    private DBService service;
    private XQMessage message;

    private String readMessage(String messageFile) throws IOException {
        InputStream in = this.getClass().getClassLoader().getResourceAsStream(messageFile);
        System.out.println(messageFile + " " + in);
        StringBuilder content = new StringBuilder();
        int ch;
        while ((ch = in.read()) != -1) {
            content.append((char) ch);
        }
        return content.toString();
    }

    private void createMessage(String content) throws XPathExpressionException, XQMessageException {
        MsgRequest msgRequest = RequestFactory.createMsgRequest();
        try {
            message = msgRequest.createXQMessage();
        } catch (ESBJUnitInitException e) {
            e.printStackTrace();
        }

        Document document = Serialise_lib1.create_xmldocument_from_string(content.toString());
        XPathFactory factory = XPathFactory.newInstance();
        XPath xpathCompiled = factory.newXPath();
        // process PARTS
        XPathExpression expr = xpathCompiled.compile("//part");
        NodeList nodeList = (NodeList) expr.evaluate(document, XPathConstants.NODESET);
        for (int i = 0; i < nodeList.getLength(); i++) {
            Node node = nodeList.item(i);
            String contentId = node.getAttributes().getNamedItem("content-id").getNodeValue();
            String contentType = node.getAttributes().getNamedItem("content-type").getNodeValue();
            String nodeValue = node.getFirstChild().getNodeValue();
            XQPart part = message.createPart();
            part.setContentId(contentId);
            part.setContent(nodeValue, contentType);
            message.addPart(part);
        }
        // process HEADERS
        expr = xpathCompiled.compile("//header");
        nodeList = (NodeList) expr.evaluate(document, XPathConstants.NODESET);
        for (int i = 0; i < nodeList.getLength(); i++) {
            Node node = nodeList.item(i);
            String name = node.getAttributes().getNamedItem("name").getNodeValue();
            String value = node.getAttributes().getNamedItem("value").getNodeValue();
            message.setHeaderValue(name, value);
        }
    }

    @Before
    public void setUp() throws IOException, XPathExpressionException, XQMessageException {
        service = new DBService();
        XQInitContextImpl ctx = new XQInitContextImpl();
        XQParameters parameters = ctx.getParameters();
        parameters.setParameter("driverClass", -1, "oracle.jdbc.driver.OracleDriver");
        parameters.setParameter("url", -1, "jdbc:oracle:thin:@esb-dev.dev.bank.ru:1521:esb");
        parameters.setParameter("user", -1, "user");
        parameters.setParameter("password", -1, "password");
        parameters.setParameter("connect", -1, "");
        parameters.setParameter("exceptions", -1, "sonicfs:///workspace/DBService/data/DBService_Exceptions.xml");
        parameters.setParameter("waiting_sec", -1, "0");
        parameters.setParameter("count_try", -1, "2");
        parameters.setParameter("sessionLifetime", -1, "300");
        parameters.setParameter("sessionNumberMin", -1, "1");
        parameters.setParameter("sessionMonitorTimer", -1, "300");
        parameters.setParameter("connectionReconnectTimeout", -1, "0");
        parameters.setParameter("autoCommit", -1, "false");
        parameters.setParameter(XQConstants.PARAM_LISTENERS, -1, "3");
        try {
            service.init(ctx);
        } catch (XQServiceException e) {
            e.printStackTrace();
        }

    }

    @After
    public void tearDown() {
    }

    @Test
    public void testLostOfConnection() throws XQServiceException, XQMessageException, IOException, XPathExpressionException {
        // 1
        String content = readMessage("Message.esbmsg");
        // 2
        createMessage(content);
        XQServiceContext ctx = new XQServiceContextImpl(message);
        XQParameters parameters = ctx.getParameters();
        parameters.setParameter("database_operation", -1,
                "../../temp/svn_ESB2015/DBService/trunk/testres/try_dbo2.xml");
        BlockingQueue<Message> queue = new ArrayBlockingQueue<>(1);
        System.out.println("1 ZZZ N1 Thread Running");
        killSession(queue);

        try {
            Message msg;
            //consuming messages until exit message is received
            while ((msg = queue.poll(10, TimeUnit.MILLISECONDS)) != null) {
                System.out.println("3 ZZZ N1 Thread Running msg=" + msg.getMsg());
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        service.service(ctx);
        XQEnvelope env = ctx.getNextIncoming();
        XQMessage msg = env.getMessage();
        System.out.println("msg: " + msg);
        XQPart part = msg.getPart("result");
        String testVal = "23";
        String testXml = "<root><output>23</output></root>";
        assertThat(msg.getHeaderValue("result")).isEqualTo(testVal);
        assertThat(part.getContent()).isEqualTo(testXml);
    }

    private void killSession(BlockingQueue<Message> queue) {
        Thread thread = new Thread() {
            public void run() {
                System.out.println("2 ZZZ N2 Thread Running");
                Message msg = new Message("we have killed oracle session");
                try {
                    queue.put(msg);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        };
        thread.start();
    }
}
