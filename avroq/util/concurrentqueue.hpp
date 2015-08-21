#ifndef __util_conqurrentqueue_
#define __util_conqurrentqueue_

#include <atomic>
#include <queue>
#include <mutex>
#include <condition_variable>

namespace util {

template <class T, int SIZE = 100>
class conqurrent_queue {
public:

	conqurrent_queue() : is_done(false) {
	}

	bool push(T &v) {
		std::unique_lock<std::mutex> lock(m);

		if (queue.size() > SIZE && !is_done) {
			full.wait(lock);
		}
		if (is_done) {
			return false;
		}

		queue.push(v);

		empty.notify_one();

	    assert(queue.size() != 0);
	    assert(v.get());

		return true;
	}

	bool pop(T &v) {
		std::unique_lock<std::mutex> lock(m);

		// TODO: remove loop
		while (queue.empty() && !is_done) {
			empty.wait(lock);
		}

		if (queue.empty() && is_done) {
			return false;
		}

	    assert(queue.size() != 0);

		v = queue.front();
		queue.pop();

	    assert(v.get());

		// if (queue.size() < (SIZE / 2)) {
			full.notify_one();
		// }

		return true;
	}

	void done() {
		std::unique_lock<std::mutex> lock(m);

		is_done = true;

		full.notify_all();
		empty.notify_all();
	}

private:
	std::queue<T> queue;
	std::mutex m;
	std::condition_variable full;
	std::condition_variable empty;
	std::atomic_bool is_done;
};

}

#endif
