import com.github.bigDataTools.util.lock.ServiceTemplate;
import com.github.bigDataTools.util.lock.LockService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CountDownLatch;

public class TestLock {
	private static final Logger LOG = LoggerFactory.getLogger(TestLock.class);
	 //确保所有线程运行结束；
	private static final int THREAD_NUM = 10;
	public  static  CountDownLatch threadSemaphore = new CountDownLatch(THREAD_NUM);

	public static void main(String[] args) {
	        for(int i=0; i < THREAD_NUM; i++){
	            final int threadId = i;
	            new Thread(){
	                @Override
	                public void run() {
	                    try{
	                    	 new LockService().doService(new ServiceTemplate() {
								@Override
								public void doService() {
									LOG.info("业务开始处理"+threadId);
									try {
										Thread.sleep(1000);
									} catch (InterruptedException e) {
										e.printStackTrace();
									}
									threadSemaphore.countDown();
								}
							});
	                    } catch (Exception e){
	                        e.printStackTrace();
	                    }
	                }
	            }.start();
	        }
	        try {
	        	threadSemaphore.await();
	            LOG.info("all thread end!");
	        } catch (Exception e) {
	            e.printStackTrace();
	        }
	    }
}
