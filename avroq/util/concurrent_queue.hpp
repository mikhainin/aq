#include <queue>
#include <mutex>
#include <condition_variable>

namespace util {

template <class T, int SIZE = 100>
class conqurrent_queue {
public:
	void push(T &v) {
		std::unique_lock<std::mutex> lock(m);

		if (queue.size() > SIZE) {
			full.wait(lock);
		}
		if (is_done) {
			return;
		}

		queue.push(v);
		empty.notify_one();
	}

	bool pop(T &v) {
		std::unique_lock<std::mutex> lock(m);

		if (queue.size() == 0 && !is_done) {
			empty.wait(lock);
		}

		if (is_done) {
			return false;
		}

		v = queue.front();
		queue.pop();

		full.notify_one();

		return true;
	}

	void done() {
		is_done = true;

		full.notify_all();
		empty.notify_all();
	}

private:
	std::queue<T> queue;
	std::mutex m;
	std::condition_variable full;
	std::condition_variable empty;
	bool is_done = false;
};

}
